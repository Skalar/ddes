/**
 * @module @ddes/core
 */

import {createHash} from 'crypto'
import {inspect} from 'util'
import {Commit} from './Commit'
import {AlreadyCommittingError, VersionConflictError} from './errors'
import {KeySchema} from './KeySchema'
import {Store} from './Store'
import {jitteredBackoff, jitteredRetry, toIso8601Timestamp} from './utils'

import {
  AggregateKey,
  AggregateKeyProps,
  AggregateKeyString,
  AggregateType,
  Event,
  EventInput,
  EventWithMetadata,
  HydrateOptions,
  InternalState,
  Iso8601Timestamp,
  JitteredRetryOptions,
} from './types'

export type AggregateStatic<T> = {
  new (): T
} & typeof Aggregate

export class Aggregate {
  /**
   * For multi-instance aggregates, a schema defining how to construct
   * an aggregate key from props.
   */
  public static readonly keySchema?: KeySchema

  /**
   * The store to use for instances of this aggregate class.
   */
  public static readonly store: Store

  /**
   * Whether or not snapshots should be used for instances of this aggregate.
   */
  public static useSnapshots: boolean

  /**
   * Snapshots will be written when aggregate version is a multiple of snapshotsFrequency
   */
  public static snapshotsFrequency: number

  public static defaultRetryOptions = {
    backoffExponent: 2,
    initialDelay: 1,
    maxDelay: 250,
    timeout: 10000,
  }

  /**
   * Calculates and returns checksum based on store upcasters and code compatibility
   */
  static get snapshotCompatibilityChecksum(): string {
    const components = []

    if (this.store.upcasters && this.store.upcasters[this.name]) {
      components.push(
        Object.keys(this.store.upcasters[this.name]).map(eventType => ({
          eventType,
          versions: Object.keys(this.store.upcasters![eventType]),
        }))
      )
    }

    return createHash('md5')
      .update(JSON.stringify(components))
      .digest('base64')
  }

  /**
   * Loads an aggregate instance
   *
   * Returns null if no commits found or state is {}
   */
  public static async load<T extends Aggregate>(
    this: AggregateStatic<T>,
    loadSpecification?: AggregateKeyProps & HydrateOptions | AggregateKeyString
  ): Promise<T | null> {
    let key: AggregateKeyString | undefined
    let hydrateOptions: HydrateOptions | undefined

    switch (typeof loadSpecification) {
      case 'string': {
        key = loadSpecification as string
        break
      }

      case 'object': {
        const {version, time, consistentRead, useSnapshots} = loadSpecification
        hydrateOptions = {
          version,
          time,
          consistentRead,
          useSnapshots,
        }

        if (loadSpecification.key) {
          key = loadSpecification.key
        } else {
          if (!this.keySchema) {
            throw new Error(`${this.name} has no keySchema`)
          }
          key = this.keySchema.keyStringFromObject(loadSpecification)
        }

        break
      }
    }

    const instance = new this(key)
    await instance.hydrate(hydrateOptions)

    if (this.keySchema && !(instance.version && instance.state)) {
      return null
    }

    return instance as T
  }

  public static async create<T extends Aggregate>(
    this: AggregateStatic<T>,
    props: object
  ) {
    if (!this.keySchema) {
      throw new Error(
        `To use ${this.name}.create(), you need to define a keySchema`
      )
    }

    const instance = new this(this.keySchema.keyStringFromObject(props))

    if (!instance.create) {
      throw new Error(`Missing create() method`)
    }

    const createParams = {
      ...props,
      ...this.keySchema.keyPropsFromObject(props),
    }

    try {
      await instance.create(createParams)
    } catch (error) {
      if (error instanceof VersionConflictError) {
        await instance.hydrate()
        await instance.create(createParams)
      } else {
        throw error
      }
    }

    return instance as T
  }

  public static async getState<T extends Aggregate>(
    this: AggregateStatic<T>,
    loadSpecification?: AggregateKeyProps & HydrateOptions | AggregateKeyString
  ) {
    const instance = await this.load(loadSpecification)

    return instance && instance.state
  }

  /**
   * Commit aggregate events to the store without loading all commits
   */
  public static async commit(
    events: EventInput[],
    retryOptions?: JitteredRetryOptions
  ): Promise<Commit>
  public static async commit(
    aggregateKey: AggregateKey,
    events: EventInput[],
    retryOptions?: JitteredRetryOptions
  ): Promise<Commit>
  public static async commit(...args: any[]): Promise<Commit> {
    let events: EventInput[]
    let retryOptions
    let key: AggregateKeyString

    if (Array.isArray(args[0])) {
      events = args[0]
      retryOptions = args[1]
      key = this.singletonKeyString
    } else {
      key =
        typeof args[0] === 'string'
          ? args[0]
          : this.keySchema!.keyStringFromObject(args[0])
      events = args[1]
      retryOptions = args[2]
    }

    return await jitteredRetry(() => this.commitEvents(key, events), {
      ...this.defaultRetryOptions,
      errorIsRetryable: error => error instanceof VersionConflictError,
      ...retryOptions,
    })
  }

  public static async *scanInstances<T extends Aggregate>(
    this: AggregateStatic<T>,
    options: {
      limit?: number
    } = {}
  ): AsyncIterableIterator<T> {
    const {limit} = options

    let currentAggregate = null

    const commits = this.store.upcastCommits(
      this.store.queryAggregateCommits({
        type: this.name,
      })
    )

    let aggregateCount = 0

    for await (const commit of commits) {
      const thisAggregateKey = commit.aggregateKey
      if (!currentAggregate || currentAggregate.key !== thisAggregateKey) {
        if (currentAggregate) {
          yield currentAggregate
          aggregateCount++
          if (limit && aggregateCount > limit) {
            return
          }
        }

        currentAggregate = new this(thisAggregateKey) as T
      }

      currentAggregate.processCommit(commit)
    }

    if (currentAggregate) {
      yield currentAggregate as T
    }
  }

  protected static singletonKeyString: AggregateKeyString = '@'

  /**
   * Function that reduces events to the desireable aggregate state
   */
  protected static stateReducer(
    internalState: InternalState,
    event: EventWithMetadata
  ): object {
    return internalState
  }

  private static async commitEvents(
    aggregateKey: AggregateKeyString,
    events: EventInput[]
  ) {
    const headCommit = await this.store.getAggregateHeadCommit!({
      type: this.name,
      key: aggregateKey,
    })
    const aggregateVersion = headCommit ? headCommit.aggregateVersion + 1 : 1

    const commit = new Commit({
      aggregateType: this.name,
      aggregateKey,
      aggregateVersion,
      events: events.map(event => ({version: 1, properties: {}, ...event})),
    })

    await this.store.commit(commit)

    return commit
  }

  public readonly type: AggregateType
  public readonly key: AggregateKeyString
  public version: number = 0
  public timestamp?: Iso8601Timestamp
  public store: Store

  protected internalState: InternalState
  protected commitInFlight?: Commit

  /**
   * @param key
   * defaults to `this.constructor.singletonKeyString`
   * @param type
   * defaults to `this.constructor.name`
   */
  constructor(key?: AggregateKeyString, type?: AggregateType) {
    const klass = this.constructor as typeof Aggregate
    this.type = type || klass.name
    this.key = key || klass.singletonKeyString
    this.store = klass.store
  }

  get state(): any {
    return this.convertFromInternalState(this.internalState)
  }

  public [inspect.custom]() {
    return this.toJSON()
  }

  public toJSON() {
    const {type, key, version, state, timestamp} = this
    return {
      type,
      key,
      version,
      state,
      timestamp,
    }
  }

  public async hydrate(options: HydrateOptions = {consistentRead: true}) {
    if (
      (options.version && options.version < this.version) ||
      (options.time && this.timestamp && this.timestamp > options.time)
    ) {
      throw new Error('You cannot hydrate to an older version')
    }

    const {
      useSnapshots = (this.constructor as typeof Aggregate).useSnapshots,
    } = options

    let shouldRewriteSnapshot = false

    if (useSnapshots) {
      const snapshot = await this.store.readSnapshot({
        type: this.type,
        key: this.key,
      })

      if (snapshot) {
        let snapshotIsUsable = false

        const klass = this.constructor as typeof Aggregate

        if (
          snapshot.compatibilityChecksum !== klass.snapshotCompatibilityChecksum
        ) {
          shouldRewriteSnapshot = true
        } else if (options.version) {
          snapshotIsUsable = options.version >= snapshot.version
        } else if (options.time) {
          snapshotIsUsable = options.time >= snapshot.timestamp
        } else if (snapshot.version > this.version) {
          snapshotIsUsable = true
        }
        if (snapshotIsUsable) {
          this.internalState = this.convertToInternalState(snapshot.state)
          this.version = snapshot.version
          this.timestamp = snapshot.timestamp
        }
      }
    }

    if (!options.version || options.version > this.version) {
      const commits = this.store.queryAggregateCommits({
        type: this.type,
        key: this.key,
        minVersion: this.version + 1,
        ...(options.version && {maxVersion: options.version}),
        ...(options.time && {
          maxTime: toIso8601Timestamp(options.time),
        }),
        ...(typeof options.consistentRead !== 'undefined' && {
          consistentRead: options.consistentRead,
        }),
      })

      for await (const commit of this.store.upcastCommits(commits)) {
        await this.processCommit(commit)
      }
    }

    if (shouldRewriteSnapshot) {
      await this.writeSnapshot()
    }
  }

  public async writeSnapshot(): Promise<void> {
    if (this.version === 0) {
      throw new Error('Cannot write snapshot for aggregate with version = 0')
    }

    const {type, key, version, state, timestamp} = this

    await this.store.writeSnapshot({
      type,
      key,
      version,
      state,
      timestamp: timestamp!,
      compatibilityChecksum: (this.constructor as typeof Aggregate)
        .snapshotCompatibilityChecksum,
    })
  }

  public async commit(
    events: EventInput[] | EventInput,
    options: {skipSnapshot?: boolean} = {}
  ): Promise<Commit> {
    if (this.commitInFlight) {
      throw new AlreadyCommittingError(
        `Already committing version ${
          this.commitInFlight.aggregateVersion
        }: ${this.commitInFlight.events.map(ev => ev.type).join(', ')}`
      )
    }

    try {
      const {type, key, version} = this
      const {store} = this.constructor as typeof Aggregate

      const commit = new Commit({
        aggregateType: type,
        aggregateKey: key,
        aggregateVersion: version + 1,
        events: (Array.isArray(events) ? events : [events]).map(event => ({
          version: 1,
          properties: {},
          ...event,
        })),
      })

      this.commitInFlight = commit
      await store.commit(commit)
      this.processCommit(commit)

      const {useSnapshots, snapshotsFrequency} = this
        .constructor as typeof Aggregate

      if (
        useSnapshots &&
        !options.skipSnapshot &&
        this.version % snapshotsFrequency === 0
      ) {
        await this.writeSnapshot()
      }

      return commit
    } finally {
      this.commitInFlight = undefined
    }
  }

  /**
   * Execute command with hydrate+retry on VersionConflictError
   */
  public async executeCommand(
    params: {
      name: string
      retryOptions?: Partial<JitteredRetryOptions>
    },
    ...commandArgs: any[]
  ) {
    if (!(this as any)[params.name]) {
      throw new Error(`no such command '${params.name}'`)
    }
    return await jitteredRetry(
      () => (this as any)[params.name](...commandArgs),
      {
        ...(this.constructor as typeof Aggregate).defaultRetryOptions,
        ...params.retryOptions,
        errorIsRetryable: error => error instanceof VersionConflictError,
        beforeRetry: () => this.hydrate(),
      }
    )
  }

  public async processCommit(commit: Commit) {
    const {
      events,
      aggregateType,
      aggregateKey,
      aggregateVersion,
      timestamp,
    } = commit

    this.internalState = events.reduce((state, event) => {
      const eventWithMetadata: EventWithMetadata = {
        ...event,
        aggregateType,
        aggregateKey,
        aggregateVersion,
        timestamp,
      }
      const klass = this.constructor as typeof Aggregate
      return klass.stateReducer(state, eventWithMetadata)
    }, this.internalState)

    this.version = commit.aggregateVersion
    this.timestamp = commit.timestamp
  }

  /**
   * Reset aggregate instance to version 0
   */
  public reset() {
    this.version = 0
    this.internalState = undefined
    this.timestamp = undefined
  }

  public async create(...args: any[]): Promise<Commit> {
    throw new Error(
      `You need to implement your own create() for ${this.constructor.name}`
    )
  }

  protected convertToInternalState(obj: object): any {
    return obj
  }

  protected convertFromInternalState(internalState: any): object {
    return this.internalState
  }
}

Object.defineProperty(Aggregate.prototype, 'state', {enumerable: true})

export default Aggregate
