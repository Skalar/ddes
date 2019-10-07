import {Firestore, Query} from '@google-cloud/firestore'

async function deleteQueryBatch(db: Firestore, query: Query): Promise<number> {
  const snapshot = await query.get()

  if (snapshot.size === 0) {
    return 0
  }

  const batch = db.batch()
  for (const doc of snapshot.docs) {
    batch.delete(doc.ref)
  }

  await batch.commit()
  const deleted = snapshot.size + (await deleteQueryBatch(db, query))

  return deleted
}

export default deleteQueryBatch
