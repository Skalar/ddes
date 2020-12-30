import {EventStore} from '@ddes/core'
import {EventStreamer} from '@ddes/event-streaming'

export interface EventStreamServer {
  resource: {
    server: EventStreamer
    port: number
  }
  teardown: () => Promise<void>
}

export async function eventStreamServer(eventStore: EventStore): Promise<EventStreamServer> {
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
