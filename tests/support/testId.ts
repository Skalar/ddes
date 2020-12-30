import {randomBytes} from 'crypto'

export const generateTestId = () => randomBytes(8).toString('hex')
