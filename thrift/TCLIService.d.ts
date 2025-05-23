//
// Autogenerated by Thrift Compiler (0.19.0)
//
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
//

import thrift = require('thrift');
import Thrift = thrift.Thrift;
import Q = thrift.Q;
import Int64 = require('node-int64');

import ttypes = require('./TCLIService_types');
import TProtocolVersion = ttypes.TProtocolVersion
import TTypeId = ttypes.TTypeId
import TSparkRowSetType = ttypes.TSparkRowSetType
import TDBSqlCompressionCodec = ttypes.TDBSqlCompressionCodec
import TDBSqlArrowLayout = ttypes.TDBSqlArrowLayout
import TStatusCode = ttypes.TStatusCode
import TOperationState = ttypes.TOperationState
import TOperationType = ttypes.TOperationType
import TGetInfoType = ttypes.TGetInfoType
import TCacheLookupResult = ttypes.TCacheLookupResult
import TFetchOrientation = ttypes.TFetchOrientation
import TJobExecutionStatus = ttypes.TJobExecutionStatus
import PRIMITIVE_TYPES = ttypes.PRIMITIVE_TYPES
import COMPLEX_TYPES = ttypes.COMPLEX_TYPES
import COLLECTION_TYPES = ttypes.COLLECTION_TYPES
import TYPE_NAMES = ttypes.TYPE_NAMES
import CHARACTER_MAXIMUM_LENGTH = ttypes.CHARACTER_MAXIMUM_LENGTH
import PRECISION = ttypes.PRECISION
import SCALE = ttypes.SCALE
import TTypeQualifierValue = ttypes.TTypeQualifierValue
import TTypeQualifiers = ttypes.TTypeQualifiers
import TPrimitiveTypeEntry = ttypes.TPrimitiveTypeEntry
import TArrayTypeEntry = ttypes.TArrayTypeEntry
import TMapTypeEntry = ttypes.TMapTypeEntry
import TStructTypeEntry = ttypes.TStructTypeEntry
import TUnionTypeEntry = ttypes.TUnionTypeEntry
import TUserDefinedTypeEntry = ttypes.TUserDefinedTypeEntry
import TTypeEntry = ttypes.TTypeEntry
import TTypeDesc = ttypes.TTypeDesc
import TColumnDesc = ttypes.TColumnDesc
import TTableSchema = ttypes.TTableSchema
import TBoolValue = ttypes.TBoolValue
import TByteValue = ttypes.TByteValue
import TI16Value = ttypes.TI16Value
import TI32Value = ttypes.TI32Value
import TI64Value = ttypes.TI64Value
import TDoubleValue = ttypes.TDoubleValue
import TStringValue = ttypes.TStringValue
import TColumnValue = ttypes.TColumnValue
import TRow = ttypes.TRow
import TBoolColumn = ttypes.TBoolColumn
import TByteColumn = ttypes.TByteColumn
import TI16Column = ttypes.TI16Column
import TI32Column = ttypes.TI32Column
import TI64Column = ttypes.TI64Column
import TDoubleColumn = ttypes.TDoubleColumn
import TStringColumn = ttypes.TStringColumn
import TBinaryColumn = ttypes.TBinaryColumn
import TColumn = ttypes.TColumn
import TDBSqlJsonArrayFormat = ttypes.TDBSqlJsonArrayFormat
import TDBSqlCsvFormat = ttypes.TDBSqlCsvFormat
import TDBSqlArrowFormat = ttypes.TDBSqlArrowFormat
import TDBSqlResultFormat = ttypes.TDBSqlResultFormat
import TSparkArrowBatch = ttypes.TSparkArrowBatch
import TSparkArrowResultLink = ttypes.TSparkArrowResultLink
import TRowSet = ttypes.TRowSet
import TStatus = ttypes.TStatus
import TNamespace = ttypes.TNamespace
import THandleIdentifier = ttypes.THandleIdentifier
import TSessionHandle = ttypes.TSessionHandle
import TOperationHandle = ttypes.TOperationHandle
import TOpenSessionReq = ttypes.TOpenSessionReq
import TOpenSessionResp = ttypes.TOpenSessionResp
import TCloseSessionReq = ttypes.TCloseSessionReq
import TCloseSessionResp = ttypes.TCloseSessionResp
import TGetInfoValue = ttypes.TGetInfoValue
import TGetInfoReq = ttypes.TGetInfoReq
import TGetInfoResp = ttypes.TGetInfoResp
import TSparkGetDirectResults = ttypes.TSparkGetDirectResults
import TSparkDirectResults = ttypes.TSparkDirectResults
import TSparkArrowTypes = ttypes.TSparkArrowTypes
import TExecuteStatementReq = ttypes.TExecuteStatementReq
import TSparkParameterValue = ttypes.TSparkParameterValue
import TSparkParameterValueArg = ttypes.TSparkParameterValueArg
import TSparkParameter = ttypes.TSparkParameter
import TStatementConf = ttypes.TStatementConf
import TExecuteStatementResp = ttypes.TExecuteStatementResp
import TGetTypeInfoReq = ttypes.TGetTypeInfoReq
import TGetTypeInfoResp = ttypes.TGetTypeInfoResp
import TGetCatalogsReq = ttypes.TGetCatalogsReq
import TGetCatalogsResp = ttypes.TGetCatalogsResp
import TGetSchemasReq = ttypes.TGetSchemasReq
import TGetSchemasResp = ttypes.TGetSchemasResp
import TGetTablesReq = ttypes.TGetTablesReq
import TGetTablesResp = ttypes.TGetTablesResp
import TGetTableTypesReq = ttypes.TGetTableTypesReq
import TGetTableTypesResp = ttypes.TGetTableTypesResp
import TGetColumnsReq = ttypes.TGetColumnsReq
import TGetColumnsResp = ttypes.TGetColumnsResp
import TGetFunctionsReq = ttypes.TGetFunctionsReq
import TGetFunctionsResp = ttypes.TGetFunctionsResp
import TGetPrimaryKeysReq = ttypes.TGetPrimaryKeysReq
import TGetPrimaryKeysResp = ttypes.TGetPrimaryKeysResp
import TGetCrossReferenceReq = ttypes.TGetCrossReferenceReq
import TGetCrossReferenceResp = ttypes.TGetCrossReferenceResp
import TGetOperationStatusReq = ttypes.TGetOperationStatusReq
import TGetOperationStatusResp = ttypes.TGetOperationStatusResp
import TCancelOperationReq = ttypes.TCancelOperationReq
import TCancelOperationResp = ttypes.TCancelOperationResp
import TCloseOperationReq = ttypes.TCloseOperationReq
import TCloseOperationResp = ttypes.TCloseOperationResp
import TGetResultSetMetadataReq = ttypes.TGetResultSetMetadataReq
import TGetResultSetMetadataResp = ttypes.TGetResultSetMetadataResp
import TFetchResultsReq = ttypes.TFetchResultsReq
import TFetchResultsResp = ttypes.TFetchResultsResp
import TGetDelegationTokenReq = ttypes.TGetDelegationTokenReq
import TGetDelegationTokenResp = ttypes.TGetDelegationTokenResp
import TCancelDelegationTokenReq = ttypes.TCancelDelegationTokenReq
import TCancelDelegationTokenResp = ttypes.TCancelDelegationTokenResp
import TRenewDelegationTokenReq = ttypes.TRenewDelegationTokenReq
import TRenewDelegationTokenResp = ttypes.TRenewDelegationTokenResp
import TProgressUpdateResp = ttypes.TProgressUpdateResp

declare class Client {
  private output: thrift.TTransport;
  private pClass: thrift.TProtocol;
  private _seqid: number;

  constructor(output: thrift.TTransport, pClass: { new(trans: thrift.TTransport): thrift.TProtocol });

  OpenSession(req: TOpenSessionReq, callback?: (error: void, response: TOpenSessionResp)=>void): void;

  CloseSession(req: TCloseSessionReq, callback?: (error: void, response: TCloseSessionResp)=>void): void;

  GetInfo(req: TGetInfoReq, callback?: (error: void, response: TGetInfoResp)=>void): void;

  ExecuteStatement(req: TExecuteStatementReq, callback?: (error: void, response: TExecuteStatementResp)=>void): void;

  GetTypeInfo(req: TGetTypeInfoReq, callback?: (error: void, response: TGetTypeInfoResp)=>void): void;

  GetCatalogs(req: TGetCatalogsReq, callback?: (error: void, response: TGetCatalogsResp)=>void): void;

  GetSchemas(req: TGetSchemasReq, callback?: (error: void, response: TGetSchemasResp)=>void): void;

  GetTables(req: TGetTablesReq, callback?: (error: void, response: TGetTablesResp)=>void): void;

  GetTableTypes(req: TGetTableTypesReq, callback?: (error: void, response: TGetTableTypesResp)=>void): void;

  GetColumns(req: TGetColumnsReq, callback?: (error: void, response: TGetColumnsResp)=>void): void;

  GetFunctions(req: TGetFunctionsReq, callback?: (error: void, response: TGetFunctionsResp)=>void): void;

  GetPrimaryKeys(req: TGetPrimaryKeysReq, callback?: (error: void, response: TGetPrimaryKeysResp)=>void): void;

  GetCrossReference(req: TGetCrossReferenceReq, callback?: (error: void, response: TGetCrossReferenceResp)=>void): void;

  GetOperationStatus(req: TGetOperationStatusReq, callback?: (error: void, response: TGetOperationStatusResp)=>void): void;

  CancelOperation(req: TCancelOperationReq, callback?: (error: void, response: TCancelOperationResp)=>void): void;

  CloseOperation(req: TCloseOperationReq, callback?: (error: void, response: TCloseOperationResp)=>void): void;

  GetResultSetMetadata(req: TGetResultSetMetadataReq, callback?: (error: void, response: TGetResultSetMetadataResp)=>void): void;

  FetchResults(req: TFetchResultsReq, callback?: (error: void, response: TFetchResultsResp)=>void): void;

  GetDelegationToken(req: TGetDelegationTokenReq, callback?: (error: void, response: TGetDelegationTokenResp)=>void): void;

  CancelDelegationToken(req: TCancelDelegationTokenReq, callback?: (error: void, response: TCancelDelegationTokenResp)=>void): void;

  RenewDelegationToken(req: TRenewDelegationTokenReq, callback?: (error: void, response: TRenewDelegationTokenResp)=>void): void;
}

declare class Processor {
  private _handler: object;

  constructor(handler: object);
  process(input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_OpenSession(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_CloseSession(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetInfo(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_ExecuteStatement(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetTypeInfo(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetCatalogs(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetSchemas(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetTables(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetTableTypes(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetColumns(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetFunctions(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetPrimaryKeys(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetCrossReference(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetOperationStatus(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_CancelOperation(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_CloseOperation(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetResultSetMetadata(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_FetchResults(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_GetDelegationToken(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_CancelDelegationToken(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
  process_RenewDelegationToken(seqid: number, input: thrift.TProtocol, output: thrift.TProtocol): void;
}
