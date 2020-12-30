/**
 * @module @ddes/aws-store
 */

/**
 * @hidden
 */
export default class ReadCapacityLimiter {
  private averageCapacityPerItem: number
  private capacityRemainder = 0
  private lastSampleSecond = 0
  private capacityLimit = 0

  constructor(capacityLimit: number, initialCapacityPerItemAssumption = 1) {
    this.averageCapacityPerItem = initialCapacityPerItemAssumption
    this.capacityLimit = capacityLimit
  }

  public get availableCapacity() {
    const thisSecond = Math.floor(Date.now() / 1000)

    if (this.lastSampleSecond !== thisSecond) {
      return this.capacityLimit
    } else {
      return this.capacityRemainder
    }
  }

  get msUntilNextWindow() {
    return 1000 - (Date.now() - Math.floor(Date.now() / 1000) * 1000)
  }

  public async getPermittedItemCount() {
    do {
      if (this.availableCapacity > 0) {
        return Math.floor(this.availableCapacity / this.averageCapacityPerItem)
      }

      await new Promise(resolve => setTimeout(resolve, this.msUntilNextWindow))
    } while (this.availableCapacity <= 0)
  }

  public registerConsumption(consumedCapacity: number, itemCount: number) {
    const thisSecond = Math.floor(Date.now() / 1000)

    if (this.lastSampleSecond === thisSecond) {
      this.capacityRemainder = this.capacityRemainder - consumedCapacity
    } else {
      this.capacityRemainder = this.capacityLimit - consumedCapacity
    }

    this.lastSampleSecond = thisSecond

    const w = Math.pow(2, -1 / 0.5)
    this.averageCapacityPerItem = w * this.averageCapacityPerItem + ((1.0 - w) * consumedCapacity) / itemCount
  }
}
