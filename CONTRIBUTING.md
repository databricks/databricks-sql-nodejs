# Contributing Guide

We happily welcome contributions to this package. We use [GitHub Issues](https://github.com/databricks/databricks-sql-nodejs/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/databricks-sql-nodejs/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication

Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
We will use the GitHub issue to discuss the feature and come to agreement.
This is to prevent your time being wasted, as well as ours.
The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.
If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.
Specifically, if the goal is to add a new extension, please read the extension policy.
Small patches and bug fixes don't need prior communication.

## Sign your work

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. The rules are pretty simple: if you can certify the below (from developercertificate.org):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Then you just add a line to every git commit message:

```
Signed-off-by: Joe Smith <joe.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.

## Project structure

- _lib/_ - source code written on TypeScript
- _dist/_ - compiled source code and must not be changed manually
- _tests/unit/_ - unit tests
- _tests/e2e/_ - end-to-end tests
- _dist/index.js_ - entry point of the driver

## Run the project

Hot compiling of TypeScript

```bash
npm run watch
```

Build the project

```bash
npm run build
```

Run the tests

```bash
npm test
npm run e2e
```

Code style:

```bash
npm run prettier
npm run prettier:fix
npm run lint
npm run lint:fix
npm run type-check
```

## Dependency Pins

A few entries in `package.json` are pinned more tightly than usual. Don't relax these without understanding why.

- **`typescript: "5.5.4"`** (exact, no caret). This pin has both a floor and a ceiling:

  - Floor (TS >= 5.0) is required because `uuid@11`'s shipped `.d.ts` uses `export type * from './types.js'`, a TS 5.0+ feature.
  - Ceiling (TS < 5.6) is required because TS 5.6 changed how `@types/node`'s generic `Buffer<TArrayBuffer extends ArrayBufferLike>` declarations get emitted into our published `dist/*.d.ts`. Allowing TS 5.6+ would leak `Buffer<ArrayBufferLike>` into the published types, which fails to compile for consumers on stale `@types/node`.
  - If you bump TS, run `npm run build` and `git diff dist/` and verify no `Buffer<...>` generics appear in any `.d.ts`. If they do, you need to either roll back or also bump `@types/node` consumer expectations (a customer-facing change).

- **`overrides.uuid: "^11.1.1"`**. Forces `thrift@0.23.0`'s declared `uuid: ^13.0.0` (ESM-only) down to v11 (dual ESM+CJS). Without this override, the driver's CJS-compiled `dist/` would crash on `require('uuid')` at runtime. Remove this override only after migrating the driver to ESM or when `thrift` drops the uuid dep.

- **`package-lock.json` is pinned to `lockfileVersion: 2`.** Modern npm writes v3 by default. To regenerate the lockfile, run `npm install --lockfile-version=2` so CI's lint step doesn't reject your PR. v2 is kept for compat with older toolchains; revisit when the team is ready to drop them.

## Pull Request Process

1. Update the [CHANGELOG](CHANGELOG.md) with details of your changes, if applicable.
2. Add any appropriate tests.
3. Make your code or other changes.
4. Follow code style: `npm run prettier:fix; npm run lint:fix`
5. Review guidelines such as
   [How to write the perfect pull request][github-perfect-pr], thanks!

[github-perfect-pr]: https://blog.github.com/2015-01-21-how-to-write-the-perfect-pull-request/
