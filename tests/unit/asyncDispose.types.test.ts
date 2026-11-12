import path from 'path';
import { expect } from 'chai';
import ts from 'typescript';

// Consumer type tests for `Symbol.asyncDispose` support: compile small consumer programs against the
// declarations emitted from `lib/` (with the `tsconfig.build.json` settings used to build `dist/`). The
// declarations and the consumer sources are kept in memory; nothing is written to disk.

const repoRoot = path.resolve(__dirname, '../..');
// Siblings of `lib/`, so the emitted `../thrift/...` imports resolve like they do from `dist/`
const declarationsDir = path.join(repoRoot, '.types-test-dist');
const consumerDir = path.join(repoRoot, '.types-test-consumer');

const virtualFiles = new Map<string, string>();

function emitDeclarations() {
  const configPath = path.join(repoRoot, 'tsconfig.build.json');
  const { config, error } = ts.readConfigFile(configPath, ts.sys.readFile);
  if (error) {
    throw new Error(ts.flattenDiagnosticMessageText(error.messageText, '\n'));
  }
  const parsed = ts.parseJsonConfigFileContent(
    config,
    ts.sys,
    repoRoot,
    { outDir: declarationsDir, emitDeclarationOnly: true, sourceMap: false, declarationMap: false },
    configPath,
  );
  const program = ts.createProgram({ rootNames: parsed.fileNames, options: parsed.options });
  const result = program.emit(undefined, (fileName, text) => {
    virtualFiles.set(path.resolve(fileName), text);
  });
  const diagnostics = [...ts.getPreEmitDiagnostics(program), ...result.diagnostics];
  expect(formatDiagnostics(diagnostics)).to.equal('');
  expect(virtualFiles.has(path.join(declarationsDir, 'index.d.ts'))).to.be.true;
}

function formatDiagnostics(diagnostics: ReadonlyArray<ts.Diagnostic>): string {
  return ts.formatDiagnostics(diagnostics, {
    getCanonicalFileName: (fileName) => fileName,
    getCurrentDirectory: () => repoRoot,
    getNewLine: () => '\n',
  });
}

function isVirtual(fileName: string): boolean {
  const resolved = path.resolve(fileName);
  return [declarationsDir, consumerDir].some((dir) => resolved === dir || resolved.startsWith(`${dir}${path.sep}`));
}

type ConsumerLib = 'es2018' | 'esnext.disposable';

// `lib.dom.d.ts` is part of TypeScript's default libs and is needed by `@types/thrift` (`XMLHttpRequest`)
const consumerOptions: Record<ConsumerLib, ts.CompilerOptions> = {
  // No disposable declarations at all: neither TypeScript's lib nor `@types/node` (the installed 18.11 typings
  // predate `Symbol.asyncDispose`) declare it, so only the driver's own declaration shim can.
  es2018: { target: ts.ScriptTarget.ES2018, lib: ['lib.es2018.d.ts', 'lib.dom.d.ts'] },
  'esnext.disposable': {
    target: ts.ScriptTarget.ES2022,
    lib: ['lib.es2022.d.ts', 'lib.esnext.disposable.d.ts', 'lib.dom.d.ts'],
  },
};

/** Type-checks `source` as a consumer of the published typings and returns the formatted errors */
function compileConsumer(source: string, consumerLib: ConsumerLib): string {
  const fileName = path.join(consumerDir, 'consumer.ts');
  virtualFiles.set(fileName, source);

  const options: ts.CompilerOptions = {
    ...consumerOptions[consumerLib],
    module: ts.ModuleKind.CommonJS,
    moduleResolution: ts.ModuleResolutionKind.Node10,
    strict: true,
    esModuleInterop: true,
    noEmit: true,
    skipLibCheck: false,
    baseUrl: repoRoot,
    paths: {
      '@databricks/sql': [path.join(declarationsDir, 'index.d.ts')],
      '@databricks/sql/dist/*': [path.join(declarationsDir, '*')],
    },
    typeRoots: [path.join(repoRoot, 'node_modules', '@types')],
    types: ['node'],
  };

  const host = ts.createCompilerHost(options);
  const { fileExists, readFile, getSourceFile, directoryExists, realpath } = host;
  host.fileExists = (name) => (isVirtual(name) ? virtualFiles.has(path.resolve(name)) : fileExists.call(host, name));
  host.readFile = (name) => (isVirtual(name) ? virtualFiles.get(path.resolve(name)) : readFile.call(host, name));
  host.directoryExists = (name) => {
    if (isVirtual(name)) {
      const dir = `${path.resolve(name)}${path.sep}`;
      return [...virtualFiles.keys()].some((file) => file.startsWith(dir));
    }
    return directoryExists ? directoryExists.call(host, name) : true;
  };
  host.realpath = (name) => (isVirtual(name) || !realpath ? name : realpath.call(host, name));
  host.getSourceFile = (name, languageVersion, onError) => {
    if (isVirtual(name)) {
      const text = virtualFiles.get(path.resolve(name));
      return text === undefined ? undefined : ts.createSourceFile(name, text, languageVersion);
    }
    return getSourceFile.call(host, name, languageVersion, onError);
  };

  const program = ts.createProgram({ rootNames: [fileName], options, host });
  return formatDiagnostics(ts.getPreEmitDiagnostics(program));
}

// Implementations and subclasses written against the contracts before async disposal was added
const legacyConsumer = `
import { Readable } from 'stream';
import { DBSQLClient, DBSQLSession } from '@databricks/sql';
import IDBSQLClient, { ConnectionOptions, OpenSessionRequest } from '@databricks/sql/dist/contracts/IDBSQLClient';
import IDBSQLSession, { ExecuteStatementOptions, CatalogsRequest } from '@databricks/sql/dist/contracts/IDBSQLSession';
import IOperation, {
  IOperationChunksIterator,
  IOperationRowsIterator,
} from '@databricks/sql/dist/contracts/IOperation';
import Status from '@databricks/sql/dist/dto/Status';

function notImplemented(): never {
  throw new Error('Not implemented');
}

const operationMock: IOperation = {
  id: 'operation',
  fetchChunk: async () => [],
  fetchAll: async () => [],
  status: async () => notImplemented(),
  cancel: async () => Status.success(),
  close: async () => Status.success(),
  finished: async () => {},
  hasMoreRows: async () => false,
  getSchema: async () => null,
  getResultMetadata: async () => notImplemented(),
  iterateChunks: (): IOperationChunksIterator => notImplemented(),
  iterateRows: (): IOperationRowsIterator => notImplemented(),
  toNodeStream: (): Readable => notImplemented(),
};

class LegacySession implements IDBSQLSession {
  public readonly id = 'session';
  public getInfo = async () => notImplemented();
  public executeStatement = async (): Promise<IOperation> => operationMock;
  public getTypeInfo = async (): Promise<IOperation> => operationMock;
  public getCatalogs = async (): Promise<IOperation> => operationMock;
  public getSchemas = async (): Promise<IOperation> => operationMock;
  public getTables = async (): Promise<IOperation> => operationMock;
  public getTableTypes = async (): Promise<IOperation> => operationMock;
  public getColumns = async (): Promise<IOperation> => operationMock;
  public getFunctions = async (): Promise<IOperation> => operationMock;
  public getPrimaryKeys = async (): Promise<IOperation> => operationMock;
  public getCrossReference = async (): Promise<IOperation> => operationMock;
  public close = async () => Status.success();
}

class LegacyClient implements IDBSQLClient {
  public async connect(): Promise<IDBSQLClient> {
    return this;
  }

  public async openSession(): Promise<IDBSQLSession> {
    return new LegacySession();
  }

  public async close(): Promise<void> {}
}

// Subclasses overriding the factories with their original return types
class LoggingClient extends DBSQLClient {
  public async connect(options: ConnectionOptions): Promise<IDBSQLClient> {
    return super.connect(options);
  }

  public async openSession(request?: OpenSessionRequest): Promise<IDBSQLSession> {
    return super.openSession(request);
  }
}

class LoggingSession extends DBSQLSession {
  public async executeStatement(statement: string, options?: ExecuteStatementOptions): Promise<IOperation> {
    return super.executeStatement(statement, options);
  }

  public async getCatalogs(request?: CatalogsRequest): Promise<IOperation> {
    return super.getCatalogs(request);
  }
}

export const clients: Array<IDBSQLClient> = [new LegacyClient(), new DBSQLClient(), new LoggingClient()];
export const sessionClass = LoggingSession;
`;

// Explicit disposal, without `await using`
const explicitDisposalConsumer = `
import { DBSQLClient, asyncDisposable, AsyncDisposableResource } from '@databricks/sql';
import IDBSQLSession from '@databricks/sql/dist/contracts/IDBSQLSession';
import IOperation from '@databricks/sql/dist/contracts/IOperation';

export async function main(): Promise<void> {
  const client = new DBSQLClient();
  await client.connect({ host: 'host', path: 'path', token: 'token' });
  const session: AsyncDisposableResource<IDBSQLSession> = asyncDisposable(await client.openSession());
  const operation: AsyncDisposableResource<IOperation> = asyncDisposable(await session.executeStatement('SELECT 1'));
  const catalogs = asyncDisposable(await session.getCatalogs());
  const disposals: Array<Promise<void>> = [
    catalogs[Symbol.asyncDispose](),
    operation[Symbol.asyncDispose](),
    session[Symbol.asyncDispose](),
    client[Symbol.asyncDispose](),
  ];
  await Promise.all(disposals);
}
`;

// Each class declaration must make \`Symbol.asyncDispose\` available on its own
const deepImportConsumers: Record<string, string> = {
  DBSQLOperation: `
import DBSQLOperation from '@databricks/sql/dist/DBSQLOperation';
export const dispose = (operation: DBSQLOperation): Promise<void> => operation[Symbol.asyncDispose]();
`,
  DBSQLSession: `
import DBSQLSession from '@databricks/sql/dist/DBSQLSession';
export const dispose = (session: DBSQLSession): Promise<void> => session[Symbol.asyncDispose]();
`,
  DBSQLClient: `
import DBSQLClient from '@databricks/sql/dist/DBSQLClient';
export const dispose = (client: DBSQLClient): Promise<void> => client[Symbol.asyncDispose]();
`,
};

const awaitUsingConsumer = `
import { DBSQLClient, asyncDisposable } from '@databricks/sql';

export async function main(): Promise<Array<object>> {
  await using client = new DBSQLClient();
  await client.connect({ host: 'host', path: 'path', token: 'token' });
  await using session = asyncDisposable(await client.openSession());
  await using operation = asyncDisposable(await session.executeStatement('SELECT 1'));
  await using tables = asyncDisposable(await session.getTables({ catalogName: 'main' }));
  await tables.fetchAll();
  return operation.fetchAll();
}
`;

describe('Symbol.asyncDispose consumer typings', function () {
  this.timeout(180000);

  before(() => {
    emitDeclarations();
  });

  after(() => {
    virtualFiles.clear();
  });

  (['es2018', 'esnext.disposable'] as const).forEach((consumerLib) => {
    it(`keeps legacy interface implementations, mocks and subclasses compiling (lib ${consumerLib})`, () => {
      expect(compileConsumer(legacyConsumer, consumerLib)).to.equal('');
    });

    it(`supports explicit disposal (lib ${consumerLib})`, () => {
      expect(compileConsumer(explicitDisposalConsumer, consumerLib)).to.equal('');
    });

    Object.entries(deepImportConsumers).forEach(([className, source]) => {
      it(`declares Symbol.asyncDispose for a deep import of ${className} (lib ${consumerLib})`, () => {
        expect(compileConsumer(source, consumerLib)).to.equal('');
      });
    });
  });

  it('supports an `await using` client -> session -> operation chain (lib esnext.disposable)', () => {
    expect(compileConsumer(awaitUsingConsumer, 'esnext.disposable')).to.equal('');
  });

  it('rejects `await using` of an interface-typed session, which may lack Symbol.asyncDispose', () => {
    const source = `
import { DBSQLClient } from '@databricks/sql';

export async function main(): Promise<void> {
  await using client = new DBSQLClient();
  await using session = await client.openSession();
}
`;
    expect(compileConsumer(source, 'esnext.disposable')).to.include('Symbol.asyncDispose');
  });
});
