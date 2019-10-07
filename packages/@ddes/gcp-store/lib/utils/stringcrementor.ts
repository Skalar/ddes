/**
 * @module @ddes/gcp-store
 */

/**
 * @hidden
 */
export default function stringcrementor(str: string, step: 1 | -1 = 1) {
  const buffer = Buffer.from(str)

  if (step === 1) {
    if (buffer[buffer.length - 1] === 255) {
      return Buffer.concat([buffer, Buffer.from(' ')]).toString()
    } else {
      buffer[buffer.length - 1]++
      return buffer.toString()
    }
  } else if (step === -1) {
    if (buffer[buffer.length - 1] <= 32) {
      return buffer.slice(0, buffer.length - 1).toString()
    } else {
      buffer[buffer.length - 1]--
      return buffer.toString()
    }
  }
}
