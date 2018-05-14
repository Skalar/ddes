/**
 * @module @ddes/core
 */

import {AggregateKey, AggregateKeyProps, KeySchemaProperty} from './types'

export default class KeySchema {
  protected properties: Array<KeySchemaProperty | string>
  protected separator: string

  constructor(
    properties: Array<KeySchemaProperty | string>,
    separator: string = '.'
  ) {
    this.properties = properties
    this.separator = separator
  }

  public keyPropsFromObject(object: any): AggregateKeyProps {
    return this.properties.reduce((keyProps, schemaProp) => {
      let name
      let value
      let optional = false

      if (typeof schemaProp === 'string') {
        name = schemaProp
        value = object[schemaProp]
      } else if (typeof schemaProp === 'object') {
        name = schemaProp.name
        value = schemaProp.value ? schemaProp.value(object) : object[name]
        optional = !!schemaProp.optional
      } else {
        throw new Error('Invalid key schema property type')
      }

      if (!optional && typeof value !== 'string') {
        throw new Error(`Value of key property '${name}' is not a string`)
      }

      return {...keyProps, [name]: value}
    }, {})
  }

  public keyStringFromKeyProps(keyProps: AggregateKeyProps) {
    return Object.values(keyProps).join(this.separator)
  }

  public keyStringFromObject(object: any): AggregateKey {
    const keyProps = this.keyPropsFromObject(object)

    return this.keyStringFromKeyProps(keyProps)
  }

  public keyPropsFromString(keyString: AggregateKey) {
    return keyString.split(this.separator).reduce(
      (props, value, i) => ({
        ...props,
        [typeof this.properties[i] === 'string'
          ? (this.properties[i] as string)
          : (this.properties[i] as KeySchemaProperty).name]: value,
      }),
      {}
    )
  }
}
