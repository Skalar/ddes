/**
 * @module @ddes/core
 */

import {createHash} from 'crypto'
import {inspect} from 'util'
import Commit from './Commit'
import KeySchema from './KeySchema'
import Store from './Store'
import {AlreadyCommittingError, VersionConflictError} from './errors'
import {
  AggregateEventUpcasters,
  AggregateKey,
  AggregateKeyProps,
  AggregateStatic,
  AggregateType,
  EventInput,
  EventWithMetadata,
  HydrateOptions,
  InternalState,
  RetryConfig,
  Timestamp,
} from './types'
import upcastCommits from './upcastCommits'
import {jitteredRetry, toTimestamp} from './utils'

export default class Aggregate {
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
   * Which chronological group to place commits in
   */
  public static chronologicalGroup: string = 'default'

  /**
   * Whether or not snapshots should be used for instances of this aggregate.
   */
  public static useSnapshots: boolean

  /**
   * Snapshots will be written when aggregate version is a multiple of snapshotsFrequency
   */
  public static snapshotsFrequency: number

  /**
   * Event upcasters to use for this aggregate
   */
  public static upcasters?: AggregateEventUpcasters

  /**
   * Whether to perform in-place store transformation of upcasted commits
   */
  public static lazyTransformation: boolean = false

  public static defaultRetryOptions = {
    backoffExponent: 2,
    initialDelay: 1,
    maxDelay: 250,
    timeout: 10000,
  }

  /**
   * Calculates and returns checksum based on store upcasters and code compatibility
   */
  static get snapshotCompatChecksum(): string {
    const components = []

    if (this.upcasters && this.upcasters[this.name]) {
      components.push(
        Object.keys(this.upcasters[this.name]).map(eventType => ({
          eventType,
          versions: Object.keys(this.upcasters![eventType]),
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
    loadSpecification?: AggregateKeyProps & HydrateOptions | AggregateKey
  ): Promise<T | null> {
    let key: AggregateKey | undefined
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
    props: object = {}
  ) {
    if (!this.keySchema) {
      throw new Error(
        `To use ${this.name}.create(), you need to define a keySchema`
      )
    }
    const keyProps = this.keySchema.keyPropsFromObject(props)
    const instance = new this(this.keySchema.keyStringFromKeyProps(keyProps))

    if (!instance.create) {
      throw new Error(`Missing create() method`)
    }

    const createParams = {
      ...props,
      ...keyProps,
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

  public static async loadOrCreate<T extends Aggregate>(
    this: AggregateStatic<T>,
    props: object
  ): Promise<T> {
    return ((await this.load(props)) as T) || ((await this.create(props)) as T)
  }

  public static async getState<T extends Aggregate>(
    this: AggregateStatic<T>,
    loadSpecification?: AggregateKeyProps & HydrateOptions | AggregateKey
  ) {
    const instance = await this.load(loadSpecification)

    return instance && instance.state
  }

  /**
   * Commit aggregate events to the store without loading all commits
   */
  public static async commit(
    events: EventInput[],
    retryOptions?: RetryConfig
  ): Promise<Commit>
  public static async commit(
    aggregateKey: AggregateKey | AggregateKeyProps,
    events: EventInput[],
    retryOptions?: RetryConfig
  ): Promise<Commit>
  public static async commit(...args: any[]): Promise<Commit> {
    let events: EventInput[]
    let retryOptions
    let key: AggregateKey

    if (Array.isArray(args[0])) {
      events = args[0]
      retryOptions = args[1]
      key = this.singletonKeyString
    } else {
      if (!this.keySchema) {
        throw new Error(
          'You cannot specify aggregateKey when Aggregate has no keySchema'
        )
      }

      key =
        typeof args[0] === 'string'
          ? args[0]
          : this.keySchema.keyStringFromObject(args[0])
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

    const commits = this.store.scanAggregateInstances(this.name, {}).commits

    let aggregateCount = 0

    for await (const commit of this.upcastCommits(commits)) {
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

  protected static singletonKeyString: AggregateKey = '@'

  protected static upcastCommits(commits: AsyncIterableIterator<Commit>) {
    const {upcasters, lazyTransformation} = this

    if (upcasters) {
      return upcastCommits(commits, upcasters, {
        lazyTransformation,
        batchMutator: this.store.createBatchMutator(),
      })
    } else {
      return commits
    }
  }

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
    aggregateKey: AggregateKey,
    events: EventInput[]
  ) {
    const headCommit = await this.store.getAggregateHeadCommit(
      this.name,
      aggregateKey
    )
    const aggregateVersion = headCommit ? headCommit.aggregateVersion + 1 : 1

    const commit = new Commit({
      aggregateType: this.name,
      aggregateKey,
      aggregateVersion,
      events: events.map(event => ({version: 1, properties: {}, ...event})),
      chronologicalGroup: this.chronologicalGroup,
    })

    await this.store.commit(commit)

    return commit
  }

  public readonly type: AggregateType
  public readonly key: AggregateKey
  public version: number = 0
  public timestamp?: Timestamp
  public store: Store

  protected internalState: InternalState
  protected commitInFlight?: Commit

  /**
   * @param key
   * defaults to `this.constructor.singletonKeyString`
   * @param type
   * defaults to `this.constructor.name`
   */
  constructor(key?: AggregateKey, type?: AggregateType) {
    const klass = this.constructor as typeof Aggregate
    this.type = type || klass.name
    this.key = key || klass.singletonKeyString
    this.store = klass.store
  }

  get state(): any {
    return this.convertFromInternalState(this.internalState)
  }

  /**
   * @hidden
   */
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
      const snapshot = await this.store.readSnapshot(this.type, this.key)

      if (snapshot) {
        let snapshotIsUsable = false

        const klass = this.constructor as typeof Aggregate

        if (snapshot.compatibilityChecksum !== klass.snapshotCompatChecksum) {
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
      const commits = this.store.queryAggregateCommits(this.type, this.key, {
        minVersion: this.version + 1,
        ...(options.version && {maxVersion: options.version}),
        ...(options.time && {
          maxTime: toTimestamp(options.time),
        }),
        ...(typeof options.consistentRead !== 'undefined' && {
          consistentRead: options.consistentRead,
        }),
      }).commits

      for await (const commit of (this
        .constructor as typeof Aggregate).upcastCommits(commits)) {
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

    await this.store.writeSnapshot(type, key, {
      version,
      state,
      timestamp: timestamp!,
      compatibilityChecksum: (this.constructor as typeof Aggregate)
        .snapshotCompatChecksum,
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
      const {store, chronologicalGroup} = this.constructor as typeof Aggregate

      const commit = new Commit({
        aggregateType: type,
        aggregateKey: key,
        aggregateVersion: version + 1,
        events: (Array.isArray(events) ? events : [events]).map(event => ({
          version: 1,
          properties: {},
          ...event,
        })),
        chronologicalGroup,
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
      retryConfig?: Partial<RetryConfig>
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
        ...params.retryConfig,
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
