import {JWT} from 'google-auth-library'
const keys = require('./auth-keys.json')

async function isIndexReady(indexPath: string, client: JWT) {
  const res = await client.request({
    url: `https://firestore.googleapis.com/v1beta2/${indexPath}`,
    method: 'GET',
  })

  if ((res.data as any).state !== 'READY') {
    console.log(`${indexPath} not ready, requesting status again in 3 sec`)
    await new Promise(resolve => setTimeout(resolve, 3000))
    await isIndexReady(indexPath, client)
  }
}

async function createCompositeIndexes(collection: string, indexes: any) {
  const client = new JWT(keys.client_email, undefined, keys.private_key, [
    'https://www.googleapis.com/auth/datastore',
    'https://www.googleapis.com/auth/cloud-platform',
  ])

  const path = `projects/${
    keys.project_id
  }/databases/(default)/collectionGroups/${collection}/indexes`

  await Promise.all(
    indexes.map(async (index: any) => {
      const res = await client.request({
        url: `https://firestore.googleapis.com/v1beta2/${path}`,
        method: 'POST',
        body: JSON.stringify({
          fields: Object.keys(index).map((path: string) => ({
            fieldPath: path,
            order: index[path],
          })),
          name: `${path}/`,
          queryScope: 'COLLECTION',
        }),
      })

      await isIndexReady((res.data as any).metadata.index, client)
    })
  )
}

createCompositeIndexes('ddes-test', [
  {p: 'ASCENDING', g: 'ASCENDING', t: 'ASCENDING'},
  {p: 'ASCENDING', g: 'DESCENDING', t: 'DESCENDING'},
  {s: 'ASCENDING', v: 'ASCENDING', t: 'ASCENDING'},
  {s: 'ASCENDING', v: 'DESCENDING', t: 'DESCENDING'},
  {p: 'DESCENDING', g: 'DESCENDING', t: 'DESCENDING'},
  {s: 'DESCENDING', v: 'DESCENDING', t: 'DESCENDING'},
])
