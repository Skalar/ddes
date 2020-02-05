// tslint:disable:max-classes-per-file

import {
  Aggregate,
  EventWithMetadata,
  VersionConflictError,
  retryCommand as retry,
} from '@ddes/core'
import {describeWithResources} from 'tests/support'

describe('Aggregate', () => {
  describeWithResources('executeCommand()', {stores: true}, context => {
    it('hydrates and retries on VersionConflictError', async () => {
      const {eventStore} = context

      class TestAggregate extends Aggregate {
        public static eventStore = eventStore

        public async myCommand() {
          return await this.commit({
            type: 'Something',
            properties: {someProperty: true},
          })
        }
      }

      const instanceA = await TestAggregate.load()
      const instanceB = await TestAggregate.load()

      await instanceA!.executeCommand({name: 'myCommand'})

      await expect(
        instanceB!.executeCommand({name: 'myCommand'})
      ).resolves.toMatchObject({aggregateVersion: 2})
    })
  })

  describeWithResources('executeCommand()', {stores: true}, context => {
    it('allows you to ensure consistency', async () => {
      const {eventStore} = context

      class TestAggregate extends Aggregate {
        public static eventStore = eventStore
        public static stateReducer(state = {}, event: EventWithMetadata) {
          switch (event.type) {
            case 'Submitted': {
              return {...state, submitted: true}
            }
            default:
              return state
          }
        }

        public async create() {
          if (this.state) {
            throw new Error('Already created')
          }

          return await this.commit({
            type: 'Submitted',
            properties: {someProperty: true},
          })
        }
      }

      const instanceA = await TestAggregate.load()
      const instanceB = await TestAggregate.load()

      await instanceA!.executeCommand({name: 'create'})

      await expect(
        instanceB!.executeCommand({name: 'create'})
      ).rejects.toMatchObject(new Error('Already created'))
    })
  })

  describeWithResources('commands', {stores: true}, context => {
    test('retryCommand decorator', async () => {
      const {eventStore} = context

      class TestAggregate extends Aggregate {
        public static eventStore = eventStore
        public static stateReducer(state = {}, event: EventWithMetadata) {
          switch (event.type) {
            case 'Submitted': {
              return {...state, submitted: true}
            }
            default:
              return state
          }
        }

        @retry()
        public async myCommand(myArg: string) {
          return await this.commit({
            type: 'SomethingHappened',
            properties: {myArg},
          })
        }

        @retry({errorIsRetryable: error => true})
        public async myOtherCommand(myArg: string) {
          try {
            return await this.commit({
              type: 'SomethingHappened',
              properties: {myArg},
            })
          } catch (error) {
            if (error instanceof VersionConflictError) {
              throw new Error('Wee')
            }
          }
        }
      }

      const instanceA = await TestAggregate.load()
      const instanceB = await TestAggregate.load()
      const instanceC = await TestAggregate.load()

      await expect(instanceA!.myCommand('hey')).resolves.toMatchObject({
        aggregateVersion: 1,
      })
      await expect(instanceB!.myCommand('ho')).resolves.toMatchObject({
        aggregateVersion: 2,
      })
      await expect(instanceC!.myCommand('hi')).resolves.toMatchObject({
        aggregateVersion: 3,
      })
    })
  })
})
