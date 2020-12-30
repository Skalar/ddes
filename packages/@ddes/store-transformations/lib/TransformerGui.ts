/**
 * @module @ddes/store-transformations
 */

import * as blessed from 'blessed'
import chalk from 'chalk'
import {randomBytes} from 'crypto'
import {tmpdir} from 'os'
import {join} from 'path'
import Transformer from './Transformer'
import {StoreState} from './types'

/**
 * Temporary GUI for transformers
 */
export default class TransformerGui {
  public transformer: Transformer
  private screen: blessed.Widgets.Screen
  private spinnerAnimation: any
  private progressBar!: blessed.Widgets.ProgressBarElement
  private table!: blessed.Widgets.TableElement
  private intervals: NodeJS.Timer[] = []
  private terminationRequestCount = 0
  private onExit?: () => void
  private spinnerAnimationFrames = [
    '▹▹▹▹▹',
    '▸▹▹▹▹',
    '▸▸▹▹▹',
    '▸▸▸▹▹',
    '▸▸▸▸▹',
    '▸▸▸▸▸',
    '▹▸▸▸▸',
    '▹▹▸▸▸',
    '▹▹▹▸▸',
    '▹▹▹▹▸',
  ]

  constructor(transformer: Transformer, onExit?: () => void) {
    this.transformer = transformer
    this.onExit = onExit

    this.screen = blessed.screen({
      smartCSR: true,
      useBCE: true,
      autoPadding: true,
      warnings: true,
      title: 'DDES EventStore Transformer',
    })

    this.setupUI()

    process.on('SIGINT', () => this.terminate)
    process.on('SIGTERM', () => this.terminate)

    this.screen.key(['escape', 'q', 'C-c'], () => {
      this.terminate()
    })

    this.screen.render()
  }

  public async terminate() {
    this.terminationRequestCount++

    if (this.terminationRequestCount > 1) {
      process.exit(0)
    }

    for (const interval of this.intervals) {
      clearInterval(interval)
    }

    this.screen.destroy()

    const stateFilePath = join(tmpdir(), randomBytes(8).toString('hex'))

    await this.transformer.writeStateFile(stateFilePath)
    // eslint-disable-next-line no-console
    console.log(`Wrote state to ${stateFilePath}`)
    // eslint-disable-next-line no-console
    console.log('Waiting for transformer termination...')

    await this.transformer.terminate()
    if (this.onExit) {
      this.onExit()
    }
  }

  private setupUI() {
    const centeredContainer = blessed.layout({
      parent: this.screen,
      align: 'center',
      layout: 'inline',
      top: 'center',
      left: 'center',
      width: 75,
      height: 25,
    })

    blessed
      .box({
        parent: centeredContainer,
        width: '100%',
      })
      .append(
        blessed.text({
          left: 'center',
          content: 'DDES EventStore Transformer',
          style: {fg: 'blue'},
        })
      )

    this.progressBar = blessed.progressbar({
      orientation: 'horizontal',
      align: 'center',
      left: 'center',
      height: 1,
      value: 10,
      bch: '─',
      pch: '─',
      style: {
        bar: {
          fg: 'green',
        },
        fg: '#555555',
      },
    } as any)

    blessed
      .box({
        parent: centeredContainer,
        width: '100%',
        padding: {bottom: 1, top: 1},
      })
      .append(this.progressBar)

    const spinner = blessed.text({
      left: 'center',
      style: {fg: 'blue'},
    })

    let spinnerFrameIndex = 0
    blessed
      .box({
        parent: centeredContainer,
        width: '100%',
        padding: {bottom: 1},
      })
      .append(spinner)

    this.intervals.push(
      (setInterval(() => {
        spinner.setContent(chalk.yellow.bold(this.spinnerAnimationFrames[spinnerFrameIndex]))
        this.screen.render()
        spinnerFrameIndex++
        if (spinnerFrameIndex >= this.spinnerAnimationFrames.length) {
          spinnerFrameIndex = 0
        }
      }, 120) as any) as NodeJS.Timer
    )

    this.table = blessed.table({
      parent: centeredContainer,
      border: 'line',
      align: 'left',
      width: 75,
      style: {
        border: {
          fg: '#333',
        },
      },
      data: this.getTableData(),
    })

    this.intervals.push(
      setInterval(() => {
        this.table.setData(this.getTableData())
        this.progressBar.setProgress(
          this.transformer.sourceCommitCount
            ? this.transformer.counters.commitsScanned / this.transformer.sourceCommitCount
            : 0
        )
      }, 1000)
    )
  }

  get sourceInfo() {
    const {transformer} = this

    switch (transformer.sourceStatus) {
      case StoreState.Unknown: {
        return `${transformer.transformation.source} ?`
      }
      case StoreState.Preparing: {
        return `${transformer.transformation.source} preparing`
      }
      case StoreState.Active: {
        return `${transformer.transformation.source} ~${transformer.sourceCommitCount}`
      }
    }
  }

  get targetInfo() {
    const {transformer} = this

    switch (transformer.targetStatus) {
      case StoreState.Unknown: {
        return `${transformer.transformation.target} ?`
      }
      case StoreState.Preparing: {
        return `${transformer.transformation.target} preparing`
      }
      case StoreState.Active: {
        return `${transformer.transformation.target} ~${transformer.targetCommitCount}`
      }
    }
  }

  get elapsedTime() {
    const diff = Date.now() - (this.transformer.executionStartedTimestamp || Date.now())

    const hours = Math.floor((diff % 86400000) / 3600000)
    const minutes = Math.round(((diff % 86400000) % 3600000) / 60000)
    const seconds = Math.round((((diff % 86400000) % 3600000) % 60000) / 1000)

    return [hours, minutes, seconds].map(v => v.toString().padStart(2, '0')).join(':')
  }

  private getTableData() {
    const {
      commitsScanned,
      commitsRead,
      commitsWritten,
      commitsDeleted,
      throttledReads,
      throttledWrites,
      /* workerInvocations, */
    } = this.transformer.counters
    return [
      [' Status', ` ${this.transformer.statusDescription}`],
      [' Transformation', ` ${this.transformer.transformation.name}`],
      [' Source', ` ${this.sourceInfo}`],
      [' Target', ` ${this.targetInfo}`],
      [' Elapsed time', ` ${this.elapsedTime}`],
      [' Commits scanned', ` ${commitsScanned.toLocaleString()} ${this.counterPerSecond('commitsScanned')}`],
      [' Commits read', ` ${commitsRead.toLocaleString()} ${this.counterPerSecond('commitsRead')}`],
      [' Commits written', ` ${commitsWritten.toLocaleString()} ${this.counterPerSecond('commitsWritten')}`],
      [' Commits deleted', ` ${commitsDeleted.toLocaleString()} ${this.counterPerSecond('commitsDeleted')}`],
      [' Throttled requests', ` read: ${throttledReads.toLocaleString()} write: ${throttledWrites.toLocaleString()}`],
    ]
  }

  private counterPerSecond(counterName: string) {
    if (!this.transformer.countersUpdatedAt) {
      return ''
    }

    const counterValue = this.transformer.counters[counterName]

    const mesurementPeriodInSeconds =
      (this.transformer.countersUpdatedAt - this.transformer.executionStartedTimestamp!) / 1000

    const value = counterValue / mesurementPeriodInSeconds

    return ` / ${(value > 1 ? Math.floor(value) : '< 1').toLocaleString()} per second`
  }
}
