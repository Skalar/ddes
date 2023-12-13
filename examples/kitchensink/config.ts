import * as stores from './stores'

export const store = stores[process.env.STORE as keyof typeof stores]

if (!store) {
	throw new Error('You need to set env variable STORE to a valid store')
}
