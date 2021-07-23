export const disposables: Array<{dispose(): any}> = []
export async function dispose() {
  await Promise.all(disposables.map(d => d.dispose()))
}
