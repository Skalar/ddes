/**
 * Utility type for creating an union event type from imported event factories
 *
 * ```typescript
 * import * as events from './events'
 * type MyEventType = ExtractEventTypes<typeof events>
 * ```
 */
export type ExtractEventTypes<T extends Record<string, any>> = ReturnType<T[keyof T]>
