const baseSha = process.argv[2]

if (baseSha === undefined) {
  throw new Error("Usage: bun scripts/check-version.ts <base-sha>")
}

const readVersion = (value: unknown): string => {
  if (value === null || typeof value !== "object" || !("version" in value) || typeof value.version !== "string") {
    throw new Error("package.json must contain a string version")
  }

  return value.version
}

const currentPackage: unknown = await Bun.file("package.json").json()
const currentVersion = readVersion(currentPackage)


const previousPackage = Bun.spawnSync(["git", "show", `${baseSha}:package.json`])
if (previousPackage.exitCode !== 0) {
  throw new Error(previousPackage.stderr.toString().trim() || `Cannot read package.json at ${baseSha}`)
}

const previousPackageJson: unknown = JSON.parse(previousPackage.stdout.toString())
const previousVersion = readVersion(previousPackageJson)
const semverPattern = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/

type ParsedVersion = {
  readonly core: readonly [number, number, number]
  readonly prerelease: ReadonlyArray<string>
}

const parseVersion = (version: string): ParsedVersion => {
  const match = semverPattern.exec(version)
  if (match === null) {
    throw new Error(`Invalid semantic version: ${version}`)
  }

  return {
    core: [Number(match[1]), Number(match[2]), Number(match[3])],
    prerelease: match[4]?.split(".") ?? []
  }
}

const compareVersions = (left: ParsedVersion, right: ParsedVersion): number => {
  for (let index = 0; index < left.core.length; index++) {
    const difference = left.core[index]! - right.core[index]!
    if (difference !== 0) return difference
  }

  if (left.prerelease.length === 0 || right.prerelease.length === 0) {
    return left.prerelease.length === right.prerelease.length ? 0 : left.prerelease.length === 0 ? 1 : -1
  }

  const length = Math.max(left.prerelease.length, right.prerelease.length)
  for (let index = 0; index < length; index++) {
    const leftPart = left.prerelease[index]
    const rightPart = right.prerelease[index]
    if (leftPart === undefined || rightPart === undefined) return leftPart === undefined ? -1 : 1
    if (leftPart === rightPart) continue

    const leftNumeric = /^\d+$/.test(leftPart)
    const rightNumeric = /^\d+$/.test(rightPart)
    if (leftNumeric && rightNumeric) return Number(leftPart) - Number(rightPart)
    if (leftNumeric !== rightNumeric) return leftNumeric ? -1 : 1
    return leftPart < rightPart ? -1 : 1
  }

  return 0
}

if (compareVersions(parseVersion(currentVersion), parseVersion(previousVersion)) <= 0) {
  throw new Error(`package.json version must advance: ${previousVersion} -> ${currentVersion}`)
}

console.log(`Version advanced: ${previousVersion} -> ${currentVersion}`)
