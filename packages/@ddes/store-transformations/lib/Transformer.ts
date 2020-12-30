/**
 * @module @ddes/store-transformations
 */

import {randomBytes} from 'crypto'
import debug from 'debug'
import {existsSync, writeFile as writeFileCb} from 'fs'
import {join} from 'path'
import {promisify} from 'util'
import Transformation from './Transformation'
import {StoreState, StoreTransformationCounters, TransformerState as State} from './types'

/**
 * @hidden
 */
const writeFile = promisify(writeFileCb)

/**
 * @hidden
 */
debug('@ddes/store-transformations:Transformer')

/**
 * Used to execute [[Transformation]]s.
 *
 * This is a basic transformer that performs all the work locally.
 *
 * ```typescript
 * const transformer = new Transformer(
 *   transformation,
 *   {
 *     workerCount: 4,
 *     readCapacityLimit: 100,
 *     writeCapacityLimit: 300,
 *   }
 * )
 *
 * await transformer.execute()
 * ```
 */
export default class Transformer {
  public counters: StoreTransformationCounters = {
    commitsScanned: 0,
    commitsRead: 0,
    commitsWritten: 0,
    commitsDeleted: 0,
    workerInvocations: 0,
    throttledReads: 0,
    throttledWrites: 0,
  }
  public sourceStatus: StoreState = StoreState.Unknown
  public sourceCommitCount = 0
  public targetStatus: StoreState = StoreState.Unknown
  public targetCommitCount = 0
  public countersUpdatedAt?: number
  public executionStartedTimestamp?: number
  public transformation: Transformation
  public state: State = State.Idle
  public workerTimeout = 30000
  public readonly workerCount: number
  public activeWorkers = 0

  protected runId: string
  protected terminationRequestedTimestamp?: number
  protected setupTarget?: boolean
  protected readCapacityLimit?: number
  protected writeCapacityLimit?: number
  protected workerStates: any[]
  protected stateFilePath?: string

  constructor(
    transformation: Transformation,
    options: {
      workerCount?: number
      setupTarget?: boolean
      readCapacityLimit?: number
      writeCapacityLimit?: number
      stateFile?: string
    } = {}
  ) {
    const {readCapacityLimit, writeCapacityLimit} = options
    this.transformation = transformation
    this.runId = `ddes-lst-${randomBytes(8).toString('hex')}`
    this.workerCount = options.workerCount || 1
    this.setupTarget = options.setupTarget
    this.readCapacityLimit = readCapacityLimit
    this.writeCapacityLimit = writeCapacityLimit

    if (options.stateFile) {
      this.stateFilePath = join(process.cwd(), options.stateFile)
    }

    if (this.stateFilePath && existsSync(this.stateFilePath)) {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const loadedStates = require(this.stateFilePath)

      if (loadedStates.length !== this.workerCount) {
        throw new Error(
          `Configured for ${this.workerCount} workers, but state file contains state for ${loadedStates.length}.`
        )
      }
      this.workerStates = loadedStates
    } else {
      this.workerStates = new Array(this.workerCount)
    }
  }

  public async execute() {
    this.executionStartedTimestamp = Date.now()

    if (this.state !== State.Idle) {
      throw new Error('Already executing')
    } else {
      this.state = State.Preparing
    }

    try {
      await this.setup()
    } catch (error) {
      await this.terminate()
      throw error
    }

    this.state = State.Running

    const workerPromises = []

    this.activeWorkers = this.workerCount
    for (let workerIndex = 0; workerIndex < this.workerCount; workerIndex++) {
      workerPromises.push(this.workerLoop(workerIndex))
    }

    try {
      await Promise.all(workerPromises)
    } catch (error) {
      await this.terminate()
      throw error
    }

    switch (this.state as State) {
      case State.Running: {
        this.state = State.Completed
        break
      }
      case State.Terminating: {
        this.state = State.Terminated
        break
      }
    }
  }

  public async terminate() {
    if (this.state === State.Terminating || this.state === State.Terminated) {
      return
    }

    this.state = State.Terminating
    this.terminationRequestedTimestamp = Date.now()
  }

  public async writeStateFile(path: string) {
    await writeFile(path, JSON.stringify(this.workerStates))
  }

  protected async setup() {
    this.sourceStatus = StoreState.Preparing
    this.targetStatus = StoreState.Preparing

    if (this.setupTarget) {
      await this.transformation.target.setup()
    }

    this.sourceCommitCount = await this.transformation.source.bestEffortCount()
    this.targetCommitCount =
      this.transformation.source === this.transformation.target
        ? this.sourceCommitCount
        : await this.transformation.target.bestEffortCount()

    this.sourceStatus = StoreState.Active
    this.targetStatus = StoreState.Active
  }

  get statusDescription() {
    switch (this.state) {
      case State.Running:
        return `running (${this.activeWorkers} local workers)`
      default:
        return this.state
    }
  }

  protected async workerLoop(index: number) {
    while (this.state === State.Running) {
      const {completed, state: newState, ...counters} = await this.transformation.perform({
        state: this.workerStates[index],
        totalSegments: this.workerCount,
        segment: index,
        deadline: Date.now() + this.workerTimeout,
        readCapacityLimit: this.readCapacityLimit ? Math.floor(this.readCapacityLimit / this.activeWorkers) : undefined,
        writeCapacityLimit: this.writeCapacityLimit
          ? Math.floor(this.writeCapacityLimit / this.activeWorkers)
          : undefined,
      })

      this.bumpCounters(counters as any)

      if (newState) {
        await this.updateWorkerState(index, newState)
      }

      if (completed) {
        this.activeWorkers--
        return
      }
    }
  }

  protected bumpCounters(counterBumps: Partial<StoreTransformationCounters>) {
    for (const [counter, increment] of Object.entries(counterBumps)) {
      if (counter && increment) {
        this.counters[counter] += increment

        switch (counter) {
          case 'commitsDeleted': {
            this.targetCommitCount -= increment
            this.sourceCommitCount -= increment
            break
          }
          case 'commitsWritten': {
            this.targetCommitCount += increment
            break
          }
        }
      }
    }
    this.countersUpdatedAt = Date.now()
  }

  protected async updateWorkerState(index: number, state: any) {
    this.workerStates[index] = state
    if (this.stateFilePath) {
      await this.writeStateFile(this.stateFilePath)
    }
  }
}
