import {Readable} from 'stream'
import GcpEventStore from '../GcpEventStore'
import GcpMetaStore from '../GcpMetaStore'
import GcpSnapshotStore from '../GcpSnapshotStore'
import {StoreQueryParams} from '../types'

export default function(
  store: GcpEventStore | GcpMetaStore | GcpSnapshotStore,
  params?: StoreQueryParams
): Readable {
  const query = store.datastore.createQuery(store.tableName, store.kind)
  if (params) {
    if (params.filters) {
      for (const {property, operator, value} of params.filters) {
        query.filter(property, operator || '=', value)
      }
    }
    if (params.orders) {
      for (const order of params.orders) {
        query.order(order.property, order.options)
      }
    }
    if (params.limit) {
      query.limit(params.limit)
    }
  }

  return store.datastore.runQueryStream(query) as Readable
}
