/**
 * @module @ddes/cli
 */

export interface CliCommand {
  description: string

  usage(): string

  params(args: object): object
  handler(params: object): Promise<void>
}
