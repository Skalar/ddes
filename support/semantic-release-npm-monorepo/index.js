const npmPlugin = require('@semantic-release/npm')
const collectPackages = require('@lerna/collect-packages')
const {readJson, writeJson, writeFileSync} = require('fs-extra')

async function prepare(pluginConfig, context) {
  const {
    nextRelease: {version},
    logger,
  } = context

  const packages = await collectPackages(process.cwd(), ['packages/**/*'])
  for (const package of packages) {
    await npmPlugin.prepare(
      {
        ...pluginConfig,
        pkgRoot: `packages/${package.name}`,
      },
      context
    )

    const packageJsonPath = `packages/${package.name}/package.json`
    const packagejson = await readJson(packageJsonPath)

    let dependenciesUpdated = 0

    for (const depKey of [
      'dependencies',
      'devDependencies',
      'peerDependencies',
      'optionalDependencies',
    ]) {
      const dependencySet = packagejson[depKey]
      if (!dependencySet) continue

      for (const pkg of packages) {
        if (dependencySet[pkg.name]) {
          dependenciesUpdated++
          dependencySet[pkg.name] = `^${version}`
        }
      }
    }

    if (dependenciesUpdated) {
      await writeJson(packageJsonPath, packagejson, {spaces: 2})
      logger.log(
        `Updated ${dependenciesUpdated} dependencies in ${packageJsonPath}`
      )
    }
  }
}

async function publish(pluginConfig, context) {
  const packages = await collectPackages(process.cwd(), ['packages/**/*'])

  for (const package of packages) {
    const pkgRoot = `packages/${package.name}`
    writeFileSync(`${pkgRoot}/.npmrc`, `//registry.npmjs.org/:_authToken=${process.env.NPM_TOKEN}`)
    await npmPlugin.publish(
      {
        ...pluginConfig,
        pkgRoot,
      },
      context
    )
  }
}

module.exports = {prepare, publish}
