# Contributing

To contribute to this repository, fork it and send pull requests.

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
npm run test
npm run e2e
```

Code style:

```bash
npm run prettier:check
npm run prettier:fix
```

## Pull Request Process

1. Update the [CHANGELOG](CHANGELOG.md) with details of your changes, if applicable.
2. Add any appropriate tests.
3. Make your code or other changes.
4. Follow code style: `npm run prettier:fix`
5. Review guidelines such as
   [How to write the perfect pull request][github-perfect-pr], thanks!

[github-perfect-pr]: https://blog.github.com/2015-01-21-how-to-write-the-perfect-pull-request/
