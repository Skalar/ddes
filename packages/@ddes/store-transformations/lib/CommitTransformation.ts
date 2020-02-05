/**
 * @module @ddes/store-transformations
 */

import {AggregateType, Commit, EventStore, MarshalledCommit} from '@ddes/core'
import debug from 'debug'
import {TransformationWorkerInput} from '.'
import Transformation from './Transformation'

/**
 * @hidden
 */
const log = debug('@ddes/store-transformations:CommitTransformation')

/**
 * For transformations operating on the [[Commit]] level.
 * Is most useful when used in conjunction with a [[Transformer]].
 *
 * ```typescript
 * export default new CommitTransformation({
 *   name: 'Do something',
 *
 *   source: new AwsEventStore({tableName: 'tableA'}),
 *   target: new AwsEventStore({tableName: 'tableB'}),
 *
 *   async transform(commit: Commit) {
 *     // return the output commits of this commit transformation
 *     return [commit]
 *   },
 * })
 * ```
 */
class CommitTransformation extends Transformation {
  public aggregateTypes?: AggregateType[]
  public marshalled?: boolean
  public marshalledKeyProps?: [string, string]
  public transform!: (
    commit: MarshalledCommit | Commit
  ) => Promise<Array<MarshalledCommit | Commit>>

  protected softInternalDeadline: number
  protected hardInternalDeadline: number

  constructor(transformationSpec: {
    name: string
    source: EventStore
    target: EventStore
    transform: (
      commit: MarshalledCommit | Commit
    ) => Promise<Array<MarshalledCommit | Commit>>
    transformerConfig?: any
    aggregateTypes?: AggregateType[]
    marshalled?: boolean
    marshalledKeyProps?: [string, string]
    softInternalDeadline?: number
    hardInternalDeadline?: number
  }) {
    const {
      aggregateTypes,
      marshalled,
      transform,
      softInternalDeadline = 10000,
      hardInternalDeadline = 1000,
      marshalledKeyProps,
      ...superParams
    } = transformationSpec
    super(superParams)

    if (marshalled && !marshalledKeyProps) {
      throw new Error(
        'You need to specify marshalledKeyProps when marshalled= true'
      )
    }
    this.aggregateTypes = aggregateTypes
    this.marshalled = marshalled
    this.marshalledKeyProps = marshalledKeyProps
    this.transform = transform
    this.softInternalDeadline = softInternalDeadline
    this.hardInternalDeadline = hardInternalDeadline
  }

  public async perform(params: TransformationWorkerInput) {
    const {
      totalSegments,
      segment,
      state,
      deadline,
      readCapacityLimit,
      writeCapacityLimit,
    } = params
    const {aggregateTypes, target, marshalled} = this

    const targetMutator = target.createBatchMutator({
      capacityLimit: writeCapacityLimit,
    })
    let completed = true
    let newState = null
    let commitsScanned = 0
    let commitsRead = 0
    let throttledReads = 0

    resultSets: for await (const resultSet of this.source.scan({
      totalSegments,
      segment,
      startKey: state,
      filterAggregateTypes: aggregateTypes,
      capacityLimit: readCapacityLimit,
    })) {
      commitsScanned += resultSet.scannedCount
      throttledReads += resultSet.throttleCount

      for await (const commit of marshalled
        ? resultSet.items
        : resultSet.commits) {
        commitsRead++

        const outputCommits = await this.transform(commit)

        if (!Array.isArray(outputCommits)) {
          throw new Error(
            'CommitTransformation#transform() must return an array'
          )
        }

        if (this.isInPlaceTransformation) {
          if (
            !outputCommits.find(outputCommit =>
              Commit.hasSameKey(outputCommit, commit)
            )
          ) {
            await targetMutator.delete(commit)
          }
        }

        await targetMutator.put(outputCommits)

        if (commit instanceof Commit) {
          newState = commit.storeKey
        } else {
          const [p, s] = this.marshalledKeyProps!
          newState = {[p]: commit[p], [s]: commit[s]}
        }

        if (deadline - Date.now() < this.hardInternalDeadline) {
          completed = false
          break resultSets
        }
      }

      if (deadline - Date.now() < this.softInternalDeadline) {
        completed = false
        break resultSets
      }
    }

    await targetMutator.drained

    return {
      completed,
      state: newState,
      commitsScanned,
      commitsRead,
      throttledReads,
      commitsWritten: targetMutator.writeCount,
      commitsDeleted: targetMutator.deleteCount,
      throttledWrites: targetMutator.throttleCount,
    }
  }
}

export default CommitTransformation
