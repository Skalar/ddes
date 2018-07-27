import {Query} from '@google-cloud/datastore/query'
import {obj as MultiStream} from 'multistream'
import {Readable} from 'stream'
import GcpEventStore from '../GcpEventStore'
import GcpMetaStore from '../GcpMetaStore'
import GcpSnapshotStore from '../GcpSnapshotStore'
import {StoreQueryParams} from '../types'

function createQuery(
  store: GcpEventStore | GcpMetaStore | GcpSnapshotStore,
  params?: StoreQueryParams
) {
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

  return query
}

export default function(
  store: GcpEventStore | GcpMetaStore | GcpSnapshotStore,
  params?: StoreQueryParams
): Readable {
  if (params && params.filterIn) {
    const {property, operator} = params.filterIn
    const queries: Query[] = []
    params.filterIn.value.forEach(val => {
      const filterIn = {
        property,
        operator,
        value: val,
      }
      queries.push(
        createQuery(store, {
          ...params,
          filters: params.filters ? [...params.filters, filterIn] : [filterIn],
        })
      )
    })

    const streams = queries.map(query => store.datastore.runQueryStream(query))

    return MultiStream(streams) as Readable
  } else {
    return store.datastore.runQueryStream(
      createQuery(store, params)
    ) as Readable
  }
}
