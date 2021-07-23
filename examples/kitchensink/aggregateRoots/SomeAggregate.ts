import {defineAggregateRoot} from '@ddes/core'
import {store} from '../config'

export const events = {
  SomethingHappened: () => ({
    type: 'SomethingHappened' as const,
  }),
}

export interface SomeAggregateState {
  ithappened: boolean
}

export const SomeAggregate = defineAggregateRoot({
  type: 'SomeAggregate' as const,
  store,
  keyProps: ['id'],
  events,
  state: (currentState: SomeAggregateState | undefined, commit, event): SomeAggregateState => {
    if (!currentState) {
      if (event.type === 'SomethingHappened') {
        return {ithappened: true}
      } else {
        throw new Error(`Missing state when we encountered ${event.type}`)
      }
    }

    switch (event.type) {
      default:
        return currentState
    }
  },
})
