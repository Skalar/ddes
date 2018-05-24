import {EventStore} from '@ddes/core'
import {EventStreamer} from '@ddes/event-streaming'

export async function eventStreamServer(context: {eventStore: EventStore}) {
  const {eventStore} = context

  const server = new EventStreamer({
    eventStore,
    port: 0,
  })
  const port = (server.wss as any)._server.address().port

  const teardown = async () => {
    server.close()
  }
  return {teardown, resource: {server, port}}
}
