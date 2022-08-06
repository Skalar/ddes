import {EventStore, AggregateCommit, AggregateEvent} from './EventStore'
import {ExtractEventTypes} from './utilityTypes'

export function defineAggregateRoot<
  TKeyProp extends string,
  TState,
  TEventFactories extends Record<string, (...args: any[]) => any>,
  TAggregateType extends string
>(config: {
  type: TAggregateType
  store: EventStore
  events: TEventFactories
  state: (
    state: TState | undefined,
    commit: AggregateCommit<ExtractEventTypes<TEventFactories>, TAggregateType>,
    event: ExtractEventTypes<typeof config.events>
  ) => TState
  keyProps: Array<TKeyProp>
  keyPropsSeparator?: string
  snapshots?: {
    write: (key: string[], data: SnapshotData<TState>) => Promise<void>
    read: (key: string[]) => Promise<SnapshotData<TState>>
    frequency: number
    compatibilityVersion: string
  }
}) {
  return new AggregateRoot<
    TState,
    ExtractEventTypes<typeof config.events>,
    Record<typeof config.keyProps[number], string | number>,
    TEventFactories,
    TAggregateType
  >(config)
}

/**
 * Provides an interface for dealing with an aggregate root type and its instances
 *
 * ```typescript
 * interface AccountState {
 *   balance: number
 * }
 *
 * type AccountEvent =
 *   | {type: 'AccountOpened'}
 *   | {type: 'MoneyDeposited'; amount: number}
 *   | {type: 'MoneyWithdrawn'; amount: number}
 *
 * interface AccountKeyProps {
 *   companyId: string
 *   accountName: string
 * }
 *
 * const Account = new AggregateRoot<AccountState, AccountEvent, AccountKeyProps>({
 *   type: 'Account',
 *   store: myEventStore,
 *   keyProps: ['companyId', 'accountName'],
 *   state: (currentState, commit, event) => {
 *     // ...
 *   }
 * })
 *
 * const account = await Account.get({companyId: '1234', accountName: 'payroll'})
 * ```
 * @typeParam TState aggregate instance state type
 * @typeParam TEvent aggregate event union type
 * @typeParam TKeyProps aggregate key properties type
 */
export class AggregateRoot<
  TState, // Aggregate instance state type
  TEvent extends AggregateEvent, // Aggregate event union type
  TKeyProps extends Record<string, string | number>, // Aggregate event union type
  TEventFactories,
  TAggregateType extends string
> {
  constructor(
    public readonly config: {
      type: TAggregateType
      store: EventStore
      events: TEventFactories
      state: (
        state: TState | undefined,
        commit: AggregateCommit<TEvent, TAggregateType>,
        event: TEvent
      ) => TState
      keyProps: Array<keyof TKeyProps>
      keyPropSeparator?: string
      snapshots?: {
        write: (key: string[], data: SnapshotData<TState>) => Promise<void>
        read: (key: string[]) => Promise<SnapshotData<TState>>
        frequency: number
        compatibilityVersion: string
      }
    }
  ) {
    if (!config.keyPropSeparator) this.config.keyPropSeparator = '.'
  }

  /**
   * Get aggregate root instance data
   *
   * ```typescript
   * Account.get('mykey')
   * Account.get({customerId: '1011', departmentId: '101})
   * ```
   */
  async get(
    key: string | TKeyProps,
    options: ({version?: number} | {time?: Date | number}) & {
      useSnapshots?: boolean
    } = {},
    data?: AggregateInstanceData<TState>
  ): Promise<AggregateInstanceData<TState> | undefined> {
    const keyString = typeof key === 'string' ? key : this.keyFromProps(key)

    const getCommitsOptions: Parameters<EventStore['queryAggregateCommits']>[2] = {}

    if ('version' in options) {
      getCommitsOptions.maxVersion = options.version
    } else if ('time' in options) {
      getCommitsOptions.maxTime = options.time
    }

    let {state = undefined, version = 0, timestamp = 0} = data || {}

    let existingSnapshotIsInvalid = false

    if (this.config.snapshots && options.useSnapshots !== false) {
      const snapshot = await this.config.snapshots.read([this.config.type, keyString])

      if (snapshot) {
        let snapshotIsUsable = false

        if (snapshot.compatibilityVersion !== this.config.snapshots.compatibilityVersion) {
          existingSnapshotIsInvalid = true
        } else if ('version' in options && options.version) {
          snapshotIsUsable = options.version >= snapshot.version
        } else if ('time' in options && options.time) {
          snapshotIsUsable = options.time >= snapshot.timestamp
        } else {
          snapshotIsUsable = true
        }

        if (snapshotIsUsable) {
          state = snapshot.state
          version = snapshot.version
          timestamp = snapshot.timestamp
        }
      }
    }

    getCommitsOptions.minVersion = version + 1

    for await (const commit of this.getCommits(keyString, getCommitsOptions)) {
      version = commit.aggregateVersion
      timestamp = commit.timestamp
      for (const event of commit.events) {
        state = this.config.state(state, commit, event)
      }
    }

    if (!version || !state) {
      return
    }

    if (
      this.config.snapshots &&
      options.useSnapshots !== false &&
      state &&
      (existingSnapshotIsInvalid || version % this.config.snapshots.frequency === 0)
    ) {
      const snapshotData: SnapshotData<TState> = {
        compatibilityVersion: this.config.snapshots.compatibilityVersion,
        state,
        timestamp,
        version,
      }

      await this.config.snapshots.write([this.config.type, keyString], snapshotData)
    }

    return {key: keyString, version, timestamp, state}
  }

  /**
   * Commit aggregate events to the store
   * TODO include info about VersionConflictError
   */
  async commit(
    key: string | TKeyProps,
    version: number,
    events: TEvent[]
  ): Promise<AggregateCommit<TEvent, TAggregateType>> {
    const aggregateKey = typeof key === 'string' ? key : this.keyFromProps(key)

    const commitData = {
      aggregateType: this.config.type,
      aggregateKey,
      aggregateVersion: version,
      events,
      timestamp: Date.now(),
    }

    const commit: AggregateCommit<TEvent, TAggregateType> = {
      ...commitData,
      chronologicalKey: this.config.store.chronologicalKey(commitData),
    }

    return await this.config.store.commit<AggregateCommit<TEvent, TAggregateType>>(commit)
  }

  /**
   * Retrieve aggregate commits for the given key
   */
  async *getCommits(
    key: string | TKeyProps,
    fetchOptions?: Parameters<EventStore['queryAggregateCommits']>[2]
  ) {
    const keyString = typeof key === 'string' ? key : this.keyFromProps(key)
    for await (const commits of this.config.store.queryAggregateCommits<
      AggregateCommit<TEvent, TAggregateType>
    >(this.config.type, keyString, fetchOptions)) {
      for (const commit of commits) yield commit
    }
  }

  /**
   * Stream current and future aggregate commits for the given key
   */
  streamCommits(
    key: string | TKeyProps
  ): AsyncIterable<Array<AggregateCommit<TEvent, TAggregateType>>>
  streamCommits(
    key: string | TKeyProps,
    minVersion: number
  ): AsyncIterable<Array<AggregateCommit<TEvent, TAggregateType>>>
  streamCommits(
    key: string | TKeyProps,
    minVersion: number,
    yieldEmpty: true
  ): AsyncIterable<Array<AggregateCommit<TEvent, TAggregateType>> | undefined>
  streamCommits(key: string | TKeyProps, minVersion = 1, yieldEmpty = false) {
    const keyString = typeof key === 'string' ? key : this.keyFromProps(key)

    return this.config.store.streamAggregateInstanceCommits<
      AggregateCommit<TEvent, TAggregateType>
    >(this.config.type, keyString, minVersion, yieldEmpty)
  }

  /**
   * Scan through aggregate roots
   */
  async *scanInstances() {
    let state = undefined
    let key = undefined
    let version = 0
    let timestamp = 0

    for await (const commits of this.config.store.scanAggregateCommitsGroupedByKey<
      AggregateCommit<TEvent, TAggregateType>
    >(this.config.type)) {
      for (const commit of commits) {
        if (key && key !== commit.aggregateKey) {
          if (state) yield {key, version, timestamp, state} as AggregateInstanceData<TState>
          state = undefined
        }

        version = commit.aggregateVersion
        timestamp = commit.timestamp
        key = commit.aggregateKey

        for (const event of commit.events) {
          state = this.config.state(state, commit, event)
        }
      }
    }

    if (version) {
      yield {key, version, timestamp, state} as AggregateInstanceData<TState>
    }
  }

  public get events() {
    return this.config.events
  }

  public propsFromKey(key: string): TKeyProps {
    const values = key.split(this.config.keyPropSeparator!)

    return this.config.keyProps.reduce(
      (r, keyProp) => ({
        ...r,
        [keyProp]: values.shift()?.toString(),
      }),
      {} as TKeyProps
    )
  }

  public keyFromProps(props: TKeyProps) {
    return this.config.keyProps
      .map(keyProp => props[keyProp].toString())
      .join(this.config.keyPropSeparator)
  }
}

export interface AggregateInstanceData<TState> {
  state: TState
  version: number
  timestamp: number
  key: string
}

export interface SnapshotData<TState> {
  state: TState
  version: number
  timestamp: number
  compatibilityVersion: string
}
