/**
 * @module @ddes/store-transformations
 */

export interface TransformationWorkerStats {
  commitsScanned: number
  commitsRead: number
  commitsWritten: number
  commitsDeleted: number
  throttledWrites: number
  throttledReads: number
}

export interface TransformationWorkerResult extends TransformationWorkerStats {
  completed: boolean
  state: any
}

export interface TransformationWorkerInput {
  state: any
  totalSegments: number
  segment: number
  deadline: number
  readCapacityLimit?: number
  writeCapacityLimit?: number
}

export interface StoreTransformationCounters extends TransformationWorkerStats {
  workerInvocations: number
  [key: string]: number
}

export enum StoreState {
  Unknown = 'unknown',
  Preparing = 'preparing',
  Active = 'active',
}

export enum TransformerState {
  Idle = 'idle',
  Preparing = 'preparing',
  Running = 'running',
  Terminating = 'terminating',
  Terminated = 'terminated',
  Completed = 'completed',
}
