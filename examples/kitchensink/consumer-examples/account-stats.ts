#!/usr/bin/env ts-node-script

import { store } from '../config'

async function main() {
	let totalActiveAccounts = 0
	let totalBalance = 0

	function updateStatsDisplay() {
		process.stdout.clearLine(0)
		process.stdout.cursorTo(0)
		process.stdout.write(
			`Active accounts: ${totalActiveAccounts} | Total balance: ${totalBalance} `,
		)
	}

	updateStatsDisplay()

	for await (const commits of store.streamCommits()) {
		for (const commit of commits) {
			switch (commit.aggregateType) {
				case 'Account': {
					for (const event of commit.events) {
						switch (event.type) {
							case 'AccountOpened':
								totalActiveAccounts += 1
								break
							case 'AccountClosed':
								totalActiveAccounts -= 1
								break
							case 'MoneyDeposited':
								totalBalance += event.amount
								break
							case 'MoneyWithdrawn':
								totalBalance -= event.amount
								break
						}
						break
					}
				}
			}
		}

		updateStatsDisplay()
	}
}

if (require.main === module) main()
