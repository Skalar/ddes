import * as async from 'async'
import 'core-js/modules/es7.symbol.async-iterator'
import {Readable, Writable} from 'stream'

interface P {
  promise: Promise<any>
  resolver: (args?: any) => void
  rejecter: (args?: any) => void
}

const makeP = (): P => {
  let resolver = () => {
    /*ignore*/
  }
  let rejecter = () => {
    /*ignore*/
  }

  const promise = new Promise((resolve, reject) => {
    resolver = resolve
    rejecter = reject
  })

  return {promise, resolver, rejecter}
}

class NextQueue {
  private cbqueue: any[] = []
  private valuequeue: any[] = []

  public save(cb: any, value: any) {
    this.cbqueue.push(cb)
    this.valuequeue.push(value)
  }
  public hasValues() {
    return this.valuequeue.length > 0
  }
  public stepValue() {
    return this.valuequeue.shift()
  }
  public stepCb() {
    if (this.cbqueue.length > 0) {
      this.cbqueue.shift()()
    }
  }
}

export default async function* asyncIterateStream(
  src: Readable | NodeJS.ReadableStream,
  objectMode: boolean
): AsyncIterableIterator<any> {
  let iterator: P | null = null
  let end: boolean = false

  src.on('error', (err: any) => {
    async.forever<string>(
      next => {
        if (iterator != null) {
          iterator.rejecter(err)
          end = true
          next('stop')
        } else {
          setTimeout(next, 1)
        }
      },
      () => {
        /*ignore*/
      }
    )
  })

  const queue = new NextQueue()
  src
    .pipe(
      new Writable({
        objectMode,
        write: (o, enc, cb) => {
          queue.save(cb, o) // save stream state only, let async while loop determine when to advance stream
          if (iterator != null) {
            iterator.resolver()
          }
        },
      })
    )
    .on('finish', () => {
      end = true
      if (iterator != null) {
        iterator.resolver()
      }
    })
  while (!end) {
    // order is very important
    iterator = makeP()
    queue.stepCb() // advance stream
    await iterator.promise // wait for stream to get next element
    iterator = null
    if (queue.hasValues()) {
      yield queue.stepValue()
    }
  }
  while (queue.hasValues()) {
    yield queue.stepValue()
  }
}
