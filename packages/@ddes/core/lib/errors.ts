/**
 * @module @ddes/core
 */

// tslint:disable:max-classes-per-file

export class VersionConflictError extends Error {
  constructor(message?: string) {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'VersionConflictError'
  }
}

export class AlreadyCommittingError extends Error {
  constructor(message?: string) {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'AlreadyCommitting'
  }
}
