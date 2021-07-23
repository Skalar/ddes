import {defineAggregateRoot} from '@ddes/core'
import {store} from '../config'

export const events = {
  AccountOpened: () => ({
    type: 'AccountOpened' as const,
  }),
  MoneyDeposited: (amount: number) => ({
    type: 'MoneyDeposited' as const,
    amount,
  }),
  MoneyWithdrawn: (amount: number) => ({
    type: 'MoneyWithdrawn' as const,
    amount,
  }),
  AccountClosed: () => ({
    type: 'AccountClosed' as const,
  }),
}

export interface AccountState {
  active: boolean
  balance: number
}

export const Account = defineAggregateRoot({
  type: 'Account',
  store,
  keyProps: ['companyId', 'accountId'],
  events,
  state: (currentState: AccountState | undefined, commit, event): AccountState => {
    if (!currentState) {
      if (event.type === 'AccountOpened') {
        return {active: true, balance: 0}
      } else {
        throw new Error(`Missing state when we encountered ${event.type}`)
      }
    }

    switch (event.type) {
      case 'MoneyDeposited': {
        currentState.balance += event.amount
        return currentState
      }

      case 'MoneyWithdrawn': {
        currentState.balance -= event.amount
        return currentState
      }

      case 'AccountClosed': {
        currentState.active = false
        return currentState
      }

      default:
        return currentState || {}
    }
  },
})
