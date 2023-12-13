#!/usr/bin/env ts-node-script

import * as stores from '../stores'
import { dispose } from './disposables'

async function main() {
	await Promise.all(Object.values(stores).map((store) => store.teardown()))
	console.log('Teardown done.')
	await dispose()
}

main()
