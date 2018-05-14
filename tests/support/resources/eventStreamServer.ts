import {Store} from '@ddes/core'
import {EventStreamer} from '@ddes/event-streaming'

export async function eventStreamServer(store: Store) {
  const server = new EventStreamer({store: (store as any) as Store, port: 0})
  const port = (server.wss as any)._server.address().port

  const teardown = async () => {
    server.close()
  }
  return {teardown, resource: {server, port}}
}
