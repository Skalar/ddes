import {describeWithResources, iterableToArray} from 'tests/support'

describeWithResources('Meta stores', {stores: true}, context => {
  test('put()', async () => {
    const {metaStore} = context

    await expect(metaStore.put(['testkey', 'option1'], false)).resolves.toBe(
      undefined
    )
    await expect(
      metaStore.put(
        ['testkey', 'option2'],
        {my: ['object']},
        {expiresAt: Date.now() + 10000}
      )
    ).resolves.toBe(undefined)

    await expect(
      metaStore.put(['testkey', 'option3'], 'should-not-exist', {
        expiresAt: new Date(),
      })
    ).resolves.toBe(undefined)
  })

  test('get()', async () => {
    const {metaStore} = context

    await expect(metaStore.get(['testkey', 'option1'])).resolves.toBe(false)

    await expect(metaStore.get(['testkey', 'option2'])).resolves.toMatchObject({
      my: ['object'],
    })

    await expect(metaStore.get(['testkey', 'option3'])).resolves.toBe(null)
  })

  test('delete()', async () => {
    const {metaStore} = context

    await expect(metaStore.delete(['testkey', 'option1'])).resolves.toBe(
      undefined
    )
  })

  test('list()', async () => {
    const {metaStore} = context
    await expect(
      iterableToArray(metaStore.list('testkey'))
    ).resolves.toMatchObject([['option2', {my: ['object']}]])
  })
})
