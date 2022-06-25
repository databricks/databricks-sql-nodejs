# Contributing

To contribute to this repository, fork it and send pull requests.

## Project structure

- *lib/* - source code written on TypeScript
- *dist/* - compiled source code and must not be changed manually
- *tests/unit/* - unit tests
- *tests/e2e/* - end-to-end tests
- *dist/index.js* - entry point of the driver

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

1. Update the [README.md](README.md) or similar documentation with details of changes you wish to make, if applicable.
2. Add any appropriate tests.
3. Make your code or other changes.
4. Review guidelines such as
   [How to write the perfect pull request][github-perfect-pr], thanks!

[angular-commit-style]: https://github.com/angular/angular.js/blob/master/DEVELOPERS.md#commits
[github-perfect-pr]: https://blog.github.com/2015-01-21-how-to-write-the-perfect-pull-request/
