#!/usr/bin/env ts-node-script

import * as stores from '../stores'
import {dispose} from './disposables'

async function main() {
  await Promise.all(Object.values(stores).map(store => store.setup()))
  console.log('Setup done.')
  await dispose()
}

main()
