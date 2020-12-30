import {EventWithMetadata} from '@ddes/core'

export default function projectorBatchMap(batches: EventWithMetadata[][]) {
  const batchMap: {[key: string]: number} = {}

  let batchNumber = 0
  for (const batch of batches) {
    for (const event of batch) {
      const key = [event.aggregateType, event.aggregateKey, event.aggregateVersion, event.type].join('.')
      batchMap[key] = batchNumber
    }
    batchNumber++
  }

  return batchMap
}
