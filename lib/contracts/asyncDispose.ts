// Declares the built-in `Symbol.asyncDispose` (identical to the declaration in TypeScript's
// `esnext.disposable` lib and in `@types/node`) so the published typings work without either.
declare global {
  interface SymbolConstructor {
    readonly asyncDispose: unique symbol;
  }
}

export {};
