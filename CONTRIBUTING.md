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

## Commit messages

Please follow the [Angular commit style][angular-commit-style].

To make it easy, just run the command

```bash
npm run commit
```

## Pull Request Process

1. Update the [CHANGELOG](CHANGELOG.md) with details of your changes, if applicable.
2. Add any appropriate tests.
3. Make your code or other changes.
4. Review guidelines such as
   [How to write the perfect pull request][github-perfect-pr], thanks!

[angular-commit-style]: https://github.com/angular/angular.js/blob/master/DEVELOPERS.md#commits
[github-perfect-pr]: https://blog.github.com/2015-01-21-how-to-write-the-perfect-pull-request/
