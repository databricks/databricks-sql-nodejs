// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { expect } from 'chai';
import { DBSQLClient, ResultFormat } from '../../lib';
import type {
  IDBSQLClient,
  ClientOptions,
  ConnectionOptions,
  OpenSessionRequest,
  IDBSQLSession,
  ExecuteStatementOptions,
  TypeInfoRequest,
  CatalogsRequest,
  SchemasRequest,
  TablesRequest,
  TableTypesRequest,
  ColumnsRequest,
  FunctionsRequest,
  PrimaryKeysRequest,
  CrossReferenceRequest,
  IOperation,
  OperationStatusCallback,
  WaitUntilReadyOptions,
  FinishedOptions,
  FetchOptions,
  GetSchemaOptions,
  IteratorOptions,
  IOperationChunksIterator,
  IOperationRowsIterator,
  NodeStreamOptions,
  IDBSQLLogger,
  LoggerOptions,
  ResultMetadata,
  DBSQLParameterValue,
} from '../../lib';
import type * as ClientContract from '../../lib/contracts/IDBSQLClient';
import type * as SessionContract from '../../lib/contracts/IDBSQLSession';
import type * as OperationContract from '../../lib/contracts/IOperation';
import type * as LoggerContract from '../../lib/contracts/IDBSQLLogger';
import type * as ResultMetadataContract from '../../lib/contracts/ResultMetadata';
import { ResultFormat as ContractResultFormat } from '../../lib/contracts/ResultMetadata';
import type * as ParameterModule from '../../lib/DBSQLParameter';

type Equals<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

// Compile-time check: each root export is the very type the contract module declares.
const sameTypes: Array<true> = [
  true as Equals<IDBSQLClient, ClientContract.default>,
  true as Equals<ClientOptions, ClientContract.ClientOptions>,
  true as Equals<ConnectionOptions, ClientContract.ConnectionOptions>,
  true as Equals<OpenSessionRequest, ClientContract.OpenSessionRequest>,
  true as Equals<IDBSQLSession, SessionContract.default>,
  true as Equals<ExecuteStatementOptions, SessionContract.ExecuteStatementOptions>,
  true as Equals<TypeInfoRequest, SessionContract.TypeInfoRequest>,
  true as Equals<CatalogsRequest, SessionContract.CatalogsRequest>,
  true as Equals<SchemasRequest, SessionContract.SchemasRequest>,
  true as Equals<TablesRequest, SessionContract.TablesRequest>,
  true as Equals<TableTypesRequest, SessionContract.TableTypesRequest>,
  true as Equals<ColumnsRequest, SessionContract.ColumnsRequest>,
  true as Equals<FunctionsRequest, SessionContract.FunctionsRequest>,
  true as Equals<PrimaryKeysRequest, SessionContract.PrimaryKeysRequest>,
  true as Equals<CrossReferenceRequest, SessionContract.CrossReferenceRequest>,
  true as Equals<IOperation, OperationContract.default>,
  true as Equals<OperationStatusCallback, OperationContract.OperationStatusCallback>,
  true as Equals<WaitUntilReadyOptions, OperationContract.WaitUntilReadyOptions>,
  true as Equals<FinishedOptions, OperationContract.FinishedOptions>,
  true as Equals<FetchOptions, OperationContract.FetchOptions>,
  true as Equals<GetSchemaOptions, OperationContract.GetSchemaOptions>,
  true as Equals<IteratorOptions, OperationContract.IteratorOptions>,
  true as Equals<IOperationChunksIterator, OperationContract.IOperationChunksIterator>,
  true as Equals<IOperationRowsIterator, OperationContract.IOperationRowsIterator>,
  true as Equals<NodeStreamOptions, OperationContract.NodeStreamOptions>,
  true as Equals<IDBSQLLogger, LoggerContract.default>,
  true as Equals<LoggerOptions, LoggerContract.LoggerOptions>,
  true as Equals<ResultMetadata, ResultMetadataContract.ResultMetadata>,
  true as Equals<DBSQLParameterValue, ParameterModule.DBSQLParameterValue>,
];

describe('package root exports', () => {
  it('exposes the client, session, and operation contract types', () => {
    const logger: IDBSQLLogger = { log: () => {} };
    const options: ClientOptions = { logger };
    const client: IDBSQLClient = new DBSQLClient(options);
    expect(client).to.be.instanceOf(DBSQLClient);
    // Always true at runtime; the real check is the `Array<true>` typing of `sameTypes`.
    expect(sameTypes.every((value) => value)).to.be.true;
  });

  it('exposes the ResultFormat enum used by ResultMetadata', () => {
    expect(ResultFormat).to.equal(ContractResultFormat);
    const metadata: Pick<ResultMetadata, 'resultFormat'> = { resultFormat: ResultFormat.ArrowBased };
    expect(metadata.resultFormat).to.equal('ARROW_BASED');
  });
});
