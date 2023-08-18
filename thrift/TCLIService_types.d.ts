//
// Autogenerated by Thrift Compiler (0.16.0)
//
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
//
import thrift = require('thrift');
import Thrift = thrift.Thrift;
import Q = thrift.Q;
import Int64 = require('node-int64');


declare enum TProtocolVersion {
  __HIVE_JDBC_WORKAROUND = -7,
  __TEST_PROTOCOL_VERSION = 65281,
  HIVE_CLI_SERVICE_PROTOCOL_V1 = 0,
  HIVE_CLI_SERVICE_PROTOCOL_V2 = 1,
  HIVE_CLI_SERVICE_PROTOCOL_V3 = 2,
  HIVE_CLI_SERVICE_PROTOCOL_V4 = 3,
  HIVE_CLI_SERVICE_PROTOCOL_V5 = 4,
  HIVE_CLI_SERVICE_PROTOCOL_V6 = 5,
  HIVE_CLI_SERVICE_PROTOCOL_V7 = 6,
  HIVE_CLI_SERVICE_PROTOCOL_V8 = 7,
  HIVE_CLI_SERVICE_PROTOCOL_V9 = 8,
  HIVE_CLI_SERVICE_PROTOCOL_V10 = 9,
  SPARK_CLI_SERVICE_PROTOCOL_V1 = 42241,
  SPARK_CLI_SERVICE_PROTOCOL_V2 = 42242,
  SPARK_CLI_SERVICE_PROTOCOL_V3 = 42243,
  SPARK_CLI_SERVICE_PROTOCOL_V4 = 42244,
  SPARK_CLI_SERVICE_PROTOCOL_V5 = 42245,
  SPARK_CLI_SERVICE_PROTOCOL_V6 = 42246,
  SPARK_CLI_SERVICE_PROTOCOL_V7 = 42247,
}

declare enum TTypeId {
  BOOLEAN_TYPE = 0,
  TINYINT_TYPE = 1,
  SMALLINT_TYPE = 2,
  INT_TYPE = 3,
  BIGINT_TYPE = 4,
  FLOAT_TYPE = 5,
  DOUBLE_TYPE = 6,
  STRING_TYPE = 7,
  TIMESTAMP_TYPE = 8,
  BINARY_TYPE = 9,
  ARRAY_TYPE = 10,
  MAP_TYPE = 11,
  STRUCT_TYPE = 12,
  UNION_TYPE = 13,
  USER_DEFINED_TYPE = 14,
  DECIMAL_TYPE = 15,
  NULL_TYPE = 16,
  DATE_TYPE = 17,
  VARCHAR_TYPE = 18,
  CHAR_TYPE = 19,
  INTERVAL_YEAR_MONTH_TYPE = 20,
  INTERVAL_DAY_TIME_TYPE = 21,
}

declare enum TSparkRowSetType {
  ARROW_BASED_SET = 0,
  COLUMN_BASED_SET = 1,
  ROW_BASED_SET = 2,
  URL_BASED_SET = 3,
}

declare enum TDBSqlCompressionCodec {
  NONE = 0,
  LZ4_FRAME = 1,
  LZ4_BLOCK = 2,
}

declare enum TDBSqlArrowLayout {
  ARROW_BATCH = 0,
  ARROW_STREAMING = 1,
}

declare enum TOperationIdempotencyType {
  UNKNOWN = 0,
  NON_IDEMPOTENT = 1,
  IDEMPOTENT = 2,
}

declare enum TOperationTimeoutLevel {
  CLUSTER = 0,
  SESSION = 1,
}

declare enum TStatusCode {
  SUCCESS_STATUS = 0,
  SUCCESS_WITH_INFO_STATUS = 1,
  STILL_EXECUTING_STATUS = 2,
  ERROR_STATUS = 3,
  INVALID_HANDLE_STATUS = 4,
}

declare enum TOperationState {
  INITIALIZED_STATE = 0,
  RUNNING_STATE = 1,
  FINISHED_STATE = 2,
  CANCELED_STATE = 3,
  CLOSED_STATE = 4,
  ERROR_STATE = 5,
  UKNOWN_STATE = 6,
  PENDING_STATE = 7,
  TIMEDOUT_STATE = 8,
}

declare enum TOperationType {
  EXECUTE_STATEMENT = 0,
  GET_TYPE_INFO = 1,
  GET_CATALOGS = 2,
  GET_SCHEMAS = 3,
  GET_TABLES = 4,
  GET_TABLE_TYPES = 5,
  GET_COLUMNS = 6,
  GET_FUNCTIONS = 7,
  UNKNOWN = 8,
}

declare enum TGetInfoType {
  CLI_MAX_DRIVER_CONNECTIONS = 0,
  CLI_MAX_CONCURRENT_ACTIVITIES = 1,
  CLI_DATA_SOURCE_NAME = 2,
  CLI_FETCH_DIRECTION = 8,
  CLI_SERVER_NAME = 13,
  CLI_SEARCH_PATTERN_ESCAPE = 14,
  CLI_DBMS_NAME = 17,
  CLI_DBMS_VER = 18,
  CLI_ACCESSIBLE_TABLES = 19,
  CLI_ACCESSIBLE_PROCEDURES = 20,
  CLI_CURSOR_COMMIT_BEHAVIOR = 23,
  CLI_DATA_SOURCE_READ_ONLY = 25,
  CLI_DEFAULT_TXN_ISOLATION = 26,
  CLI_IDENTIFIER_CASE = 28,
  CLI_IDENTIFIER_QUOTE_CHAR = 29,
  CLI_MAX_COLUMN_NAME_LEN = 30,
  CLI_MAX_CURSOR_NAME_LEN = 31,
  CLI_MAX_SCHEMA_NAME_LEN = 32,
  CLI_MAX_CATALOG_NAME_LEN = 34,
  CLI_MAX_TABLE_NAME_LEN = 35,
  CLI_SCROLL_CONCURRENCY = 43,
  CLI_TXN_CAPABLE = 46,
  CLI_USER_NAME = 47,
  CLI_TXN_ISOLATION_OPTION = 72,
  CLI_INTEGRITY = 73,
  CLI_GETDATA_EXTENSIONS = 81,
  CLI_NULL_COLLATION = 85,
  CLI_ALTER_TABLE = 86,
  CLI_ORDER_BY_COLUMNS_IN_SELECT = 90,
  CLI_SPECIAL_CHARACTERS = 94,
  CLI_MAX_COLUMNS_IN_GROUP_BY = 97,
  CLI_MAX_COLUMNS_IN_INDEX = 98,
  CLI_MAX_COLUMNS_IN_ORDER_BY = 99,
  CLI_MAX_COLUMNS_IN_SELECT = 100,
  CLI_MAX_COLUMNS_IN_TABLE = 101,
  CLI_MAX_INDEX_SIZE = 102,
  CLI_MAX_ROW_SIZE = 104,
  CLI_MAX_STATEMENT_LEN = 105,
  CLI_MAX_TABLES_IN_SELECT = 106,
  CLI_MAX_USER_NAME_LEN = 107,
  CLI_OJ_CAPABILITIES = 115,
  CLI_XOPEN_CLI_YEAR = 10000,
  CLI_CURSOR_SENSITIVITY = 10001,
  CLI_DESCRIBE_PARAMETER = 10002,
  CLI_CATALOG_NAME = 10003,
  CLI_COLLATION_SEQ = 10004,
  CLI_MAX_IDENTIFIER_LEN = 10005,
}

declare enum TResultPersistenceMode {
  ONLY_LARGE_RESULTS = 0,
  ALL_QUERY_RESULTS = 1,
  ALL_RESULTS = 2,
}

declare enum TCacheLookupResult {
  CACHE_INELIGIBLE = 0,
  LOCAL_CACHE_HIT = 1,
  REMOTE_CACHE_HIT = 2,
  CACHE_MISS = 3,
}

declare enum TCloudFetchDisabledReason {
  ARROW_SUPPORT = 0,
  CLOUD_FETCH_SUPPORT = 1,
  PROTOCOL_VERSION = 2,
  REGION_SUPPORT = 3,
  BLOCKLISTED_OPERATION = 4,
  SMALL_RESULT_SIZE = 5,
  CUSTOMER_STORAGE_SUPPORT = 6,
  UNKNOWN = 7,
}

declare enum TDBSqlManifestFileFormat {
  THRIFT_GET_RESULT_SET_METADATA_RESP = 0,
}

declare enum TFetchOrientation {
  FETCH_NEXT = 0,
  FETCH_PRIOR = 1,
  FETCH_RELATIVE = 2,
  FETCH_ABSOLUTE = 3,
  FETCH_FIRST = 4,
  FETCH_LAST = 5,
}

declare enum TDBSqlFetchDisposition {
  DISPOSITION_UNSPECIFIED = 0,
  DISPOSITION_INLINE = 1,
  DISPOSITION_EXTERNAL_LINKS = 2,
  DISPOSITION_INTERNAL_DBFS = 3,
}

declare enum TJobExecutionStatus {
  IN_PROGRESS = 0,
  COMPLETE = 1,
  NOT_AVAILABLE = 2,
}

declare class TTypeQualifierValue {
  public i32Value?: number;
  public stringValue?: string;

    constructor(args?: { i32Value?: number; stringValue?: string; });
}

declare class TTypeQualifiers {
  public qualifiers: { [k: string]: TTypeQualifierValue; };

    constructor(args?: { qualifiers: { [k: string]: TTypeQualifierValue; }; });
}

declare class TPrimitiveTypeEntry {
  public type: TTypeId;
  public typeQualifiers?: TTypeQualifiers;

    constructor(args?: { type: TTypeId; typeQualifiers?: TTypeQualifiers; });
}

declare class TArrayTypeEntry {
  public objectTypePtr: number;

    constructor(args?: { objectTypePtr: number; });
}

declare class TMapTypeEntry {
  public keyTypePtr: number;
  public valueTypePtr: number;

    constructor(args?: { keyTypePtr: number; valueTypePtr: number; });
}

declare class TStructTypeEntry {
  public nameToTypePtr: { [k: string]: number; };

    constructor(args?: { nameToTypePtr: { [k: string]: number; }; });
}

declare class TUnionTypeEntry {
  public nameToTypePtr: { [k: string]: number; };

    constructor(args?: { nameToTypePtr: { [k: string]: number; }; });
}

declare class TUserDefinedTypeEntry {
  public typeClassName: string;

    constructor(args?: { typeClassName: string; });
}

declare class TTypeEntry {
  public primitiveEntry?: TPrimitiveTypeEntry;
  public arrayEntry?: TArrayTypeEntry;
  public mapEntry?: TMapTypeEntry;
  public structEntry?: TStructTypeEntry;
  public unionEntry?: TUnionTypeEntry;
  public userDefinedTypeEntry?: TUserDefinedTypeEntry;

    constructor(args?: { primitiveEntry?: TPrimitiveTypeEntry; arrayEntry?: TArrayTypeEntry; mapEntry?: TMapTypeEntry; structEntry?: TStructTypeEntry; unionEntry?: TUnionTypeEntry; userDefinedTypeEntry?: TUserDefinedTypeEntry; });
}

declare class TTypeDesc {
  public types: TTypeEntry[];

    constructor(args?: { types: TTypeEntry[]; });
}

declare class TColumnDesc {
  public columnName: string;
  public typeDesc: TTypeDesc;
  public position: number;
  public comment?: string;

    constructor(args?: { columnName: string; typeDesc: TTypeDesc; position: number; comment?: string; });
}

declare class TTableSchema {
  public columns: TColumnDesc[];

    constructor(args?: { columns: TColumnDesc[]; });
}

declare class TBoolValue {
  public value?: boolean;

    constructor(args?: { value?: boolean; });
}

declare class TByteValue {
  public value?: any;

    constructor(args?: { value?: any; });
}

declare class TI16Value {
  public value?: number;

    constructor(args?: { value?: number; });
}

declare class TI32Value {
  public value?: number;

    constructor(args?: { value?: number; });
}

declare class TI64Value {
  public value?: Int64;

    constructor(args?: { value?: Int64; });
}

declare class TDoubleValue {
  public value?: number;

    constructor(args?: { value?: number; });
}

declare class TStringValue {
  public value?: string;

    constructor(args?: { value?: string; });
}

declare class TColumnValue {
  public boolVal?: TBoolValue;
  public byteVal?: TByteValue;
  public i16Val?: TI16Value;
  public i32Val?: TI32Value;
  public i64Val?: TI64Value;
  public doubleVal?: TDoubleValue;
  public stringVal?: TStringValue;

    constructor(args?: { boolVal?: TBoolValue; byteVal?: TByteValue; i16Val?: TI16Value; i32Val?: TI32Value; i64Val?: TI64Value; doubleVal?: TDoubleValue; stringVal?: TStringValue; });
}

declare class TRow {
  public colVals: TColumnValue[];

    constructor(args?: { colVals: TColumnValue[]; });
}

declare class TBoolColumn {
  public values: boolean[];
  public nulls: Buffer;

    constructor(args?: { values: boolean[]; nulls: Buffer; });
}

declare class TByteColumn {
  public values: any[];
  public nulls: Buffer;

    constructor(args?: { values: any[]; nulls: Buffer; });
}

declare class TI16Column {
  public values: number[];
  public nulls: Buffer;

    constructor(args?: { values: number[]; nulls: Buffer; });
}

declare class TI32Column {
  public values: number[];
  public nulls: Buffer;

    constructor(args?: { values: number[]; nulls: Buffer; });
}

declare class TI64Column {
  public values: Int64[];
  public nulls: Buffer;

    constructor(args?: { values: Int64[]; nulls: Buffer; });
}

declare class TDoubleColumn {
  public values: number[];
  public nulls: Buffer;

    constructor(args?: { values: number[]; nulls: Buffer; });
}

declare class TStringColumn {
  public values: string[];
  public nulls: Buffer;

    constructor(args?: { values: string[]; nulls: Buffer; });
}

declare class TBinaryColumn {
  public values: Buffer[];
  public nulls: Buffer;

    constructor(args?: { values: Buffer[]; nulls: Buffer; });
}

declare class TColumn {
  public boolVal?: TBoolColumn;
  public byteVal?: TByteColumn;
  public i16Val?: TI16Column;
  public i32Val?: TI32Column;
  public i64Val?: TI64Column;
  public doubleVal?: TDoubleColumn;
  public stringVal?: TStringColumn;
  public binaryVal?: TBinaryColumn;

    constructor(args?: { boolVal?: TBoolColumn; byteVal?: TByteColumn; i16Val?: TI16Column; i32Val?: TI32Column; i64Val?: TI64Column; doubleVal?: TDoubleColumn; stringVal?: TStringColumn; binaryVal?: TBinaryColumn; });
}

declare class TDBSqlJsonArrayFormat {
  public compressionCodec?: TDBSqlCompressionCodec;

    constructor(args?: { compressionCodec?: TDBSqlCompressionCodec; });
}

declare class TDBSqlCsvFormat {
  public compressionCodec?: TDBSqlCompressionCodec;

    constructor(args?: { compressionCodec?: TDBSqlCompressionCodec; });
}

declare class TDBSqlArrowFormat {
  public arrowLayout?: TDBSqlArrowLayout;
  public compressionCodec?: TDBSqlCompressionCodec;

    constructor(args?: { arrowLayout?: TDBSqlArrowLayout; compressionCodec?: TDBSqlCompressionCodec; });
}

declare class TDBSqlResultFormat {
  public arrowFormat?: TDBSqlArrowFormat;
  public csvFormat?: TDBSqlCsvFormat;
  public jsonArrayFormat?: TDBSqlJsonArrayFormat;

    constructor(args?: { arrowFormat?: TDBSqlArrowFormat; csvFormat?: TDBSqlCsvFormat; jsonArrayFormat?: TDBSqlJsonArrayFormat; });
}

declare class TSparkArrowBatch {
  public batch: Buffer;
  public rowCount: Int64;

    constructor(args?: { batch: Buffer; rowCount: Int64; });
}

declare class TSparkArrowResultLink {
  public fileLink: string;
  public expiryTime: Int64;
  public startRowOffset: Int64;
  public rowCount: Int64;
  public bytesNum: Int64;
  public httpHeaders?: { [k: string]: string; };

    constructor(args?: { fileLink: string; expiryTime: Int64; startRowOffset: Int64; rowCount: Int64; bytesNum: Int64; httpHeaders?: { [k: string]: string; }; });
}

declare class TDBSqlCloudResultFile {
  public filePath?: string;
  public startRowOffset?: Int64;
  public rowCount?: Int64;
  public uncompressedBytes?: Int64;
  public compressedBytes?: Int64;
  public fileLink?: string;
  public linkExpiryTime?: Int64;
  public httpHeaders?: { [k: string]: string; };

    constructor(args?: { filePath?: string; startRowOffset?: Int64; rowCount?: Int64; uncompressedBytes?: Int64; compressedBytes?: Int64; fileLink?: string; linkExpiryTime?: Int64; httpHeaders?: { [k: string]: string; }; });
}

declare class TRowSet {
  public startRowOffset: Int64;
  public rows: TRow[];
  public columns?: TColumn[];
  public binaryColumns?: Buffer;
  public columnCount?: number;
  public arrowBatches?: TSparkArrowBatch[];
  public resultLinks?: TSparkArrowResultLink[];
  public cloudFetchResults?: TDBSqlCloudResultFile[];

    constructor(args?: { startRowOffset: Int64; rows: TRow[]; columns?: TColumn[]; binaryColumns?: Buffer; columnCount?: number; arrowBatches?: TSparkArrowBatch[]; resultLinks?: TSparkArrowResultLink[]; cloudFetchResults?: TDBSqlCloudResultFile[]; });
}

declare class TDBSqlTempView {
  public name?: string;
  public sqlStatement?: string;
  public properties?: { [k: string]: string; };
  public viewSchema?: string;

    constructor(args?: { name?: string; sqlStatement?: string; properties?: { [k: string]: string; }; viewSchema?: string; });
}

declare class TDBSqlSessionCapabilities {
  public supportsMultipleCatalogs?: boolean;

    constructor(args?: { supportsMultipleCatalogs?: boolean; });
}

declare class TExpressionInfo {
  public className?: string;
  public usage?: string;
  public name?: string;
  public extended?: string;
  public db?: string;
  public arguments?: string;
  public examples?: string;
  public note?: string;
  public group?: string;
  public since?: string;
  public deprecated?: string;
  public source?: string;

    constructor(args?: { className?: string; usage?: string; name?: string; extended?: string; db?: string; arguments?: string; examples?: string; note?: string; group?: string; since?: string; deprecated?: string; source?: string; });
}

declare class TDBSqlConfValue {
  public value?: string;

    constructor(args?: { value?: string; });
}

declare class TDBSqlSessionConf {
  public confs?: { [k: string]: string; };
  public tempViews?: TDBSqlTempView[];
  public currentDatabase?: string;
  public currentCatalog?: string;
  public sessionCapabilities?: TDBSqlSessionCapabilities;
  public expressionsInfos?: TExpressionInfo[];
  public internalConfs?: { [k: string]: TDBSqlConfValue; };

    constructor(args?: { confs?: { [k: string]: string; }; tempViews?: TDBSqlTempView[]; currentDatabase?: string; currentCatalog?: string; sessionCapabilities?: TDBSqlSessionCapabilities; expressionsInfos?: TExpressionInfo[]; internalConfs?: { [k: string]: TDBSqlConfValue; }; });
}

declare class TStatus {
  public statusCode: TStatusCode;
  public infoMessages?: string[];
  public sqlState?: string;
  public errorCode?: number;
  public errorMessage?: string;
  public displayMessage?: string;
  public errorDetailsJson?: string;
  public responseValidation?: Buffer;

    constructor(args?: { statusCode: TStatusCode; infoMessages?: string[]; sqlState?: string; errorCode?: number; errorMessage?: string; displayMessage?: string; errorDetailsJson?: string; responseValidation?: Buffer; });
}

declare class TNamespace {
  public catalogName?: string;
  public schemaName?: string;

    constructor(args?: { catalogName?: string; schemaName?: string; });
}

declare class THandleIdentifier {
  public guid: Buffer;
  public secret: Buffer;
  public executionVersion?: number;

    constructor(args?: { guid: Buffer; secret: Buffer; executionVersion?: number; });
}

declare class TSessionHandle {
  public sessionId: THandleIdentifier;
  public serverProtocolVersion?: TProtocolVersion;

    constructor(args?: { sessionId: THandleIdentifier; serverProtocolVersion?: TProtocolVersion; });
}

declare class TOperationHandle {
  public operationId: THandleIdentifier;
  public operationType: TOperationType;
  public hasResultSet: boolean;
  public modifiedRowCount?: number;

    constructor(args?: { operationId: THandleIdentifier; operationType: TOperationType; hasResultSet: boolean; modifiedRowCount?: number; });
}

declare class TOpenSessionReq {
  public client_protocol?: TProtocolVersion;
  public username?: string;
  public password?: string;
  public configuration?: { [k: string]: string; };
  public getInfos?: TGetInfoType[];
  public client_protocol_i64?: Int64;
  public connectionProperties?: { [k: string]: string; };
  public initialNamespace?: TNamespace;
  public canUseMultipleCatalogs?: boolean;
  public sessionId?: THandleIdentifier;

    constructor(args?: { client_protocol?: TProtocolVersion; username?: string; password?: string; configuration?: { [k: string]: string; }; getInfos?: TGetInfoType[]; client_protocol_i64?: Int64; connectionProperties?: { [k: string]: string; }; initialNamespace?: TNamespace; canUseMultipleCatalogs?: boolean; sessionId?: THandleIdentifier; });
}

declare class TOpenSessionResp {
  public status: TStatus;
  public serverProtocolVersion: TProtocolVersion;
  public sessionHandle?: TSessionHandle;
  public configuration?: { [k: string]: string; };
  public initialNamespace?: TNamespace;
  public canUseMultipleCatalogs?: boolean;
  public getInfos?: TGetInfoValue[];

    constructor(args?: { status: TStatus; serverProtocolVersion: TProtocolVersion; sessionHandle?: TSessionHandle; configuration?: { [k: string]: string; }; initialNamespace?: TNamespace; canUseMultipleCatalogs?: boolean; getInfos?: TGetInfoValue[]; });
}

declare class TCloseSessionReq {
  public sessionHandle: TSessionHandle;

    constructor(args?: { sessionHandle: TSessionHandle; });
}

declare class TCloseSessionResp {
  public status: TStatus;

    constructor(args?: { status: TStatus; });
}

declare class TGetInfoValue {
  public stringValue?: string;
  public smallIntValue?: number;
  public integerBitmask?: number;
  public integerFlag?: number;
  public binaryValue?: number;
  public lenValue?: Int64;

    constructor(args?: { stringValue?: string; smallIntValue?: number; integerBitmask?: number; integerFlag?: number; binaryValue?: number; lenValue?: Int64; });
}

declare class TGetInfoReq {
  public sessionHandle: TSessionHandle;
  public infoType: TGetInfoType;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; infoType: TGetInfoType; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetInfoResp {
  public status: TStatus;
  public infoValue: TGetInfoValue;

    constructor(args?: { status: TStatus; infoValue: TGetInfoValue; });
}

declare class TSparkGetDirectResults {
  public maxRows: Int64;
  public maxBytes?: Int64;

    constructor(args?: { maxRows: Int64; maxBytes?: Int64; });
}

declare class TSparkDirectResults {
  public operationStatus?: TGetOperationStatusResp;
  public resultSetMetadata?: TGetResultSetMetadataResp;
  public resultSet?: TFetchResultsResp;
  public closeOperation?: TCloseOperationResp;

    constructor(args?: { operationStatus?: TGetOperationStatusResp; resultSetMetadata?: TGetResultSetMetadataResp; resultSet?: TFetchResultsResp; closeOperation?: TCloseOperationResp; });
}

declare class TSparkArrowTypes {
  public timestampAsArrow?: boolean;
  public decimalAsArrow?: boolean;
  public complexTypesAsArrow?: boolean;
  public intervalTypesAsArrow?: boolean;
  public nullTypeAsArrow?: boolean;

    constructor(args?: { timestampAsArrow?: boolean; decimalAsArrow?: boolean; complexTypesAsArrow?: boolean; intervalTypesAsArrow?: boolean; nullTypeAsArrow?: boolean; });
}

declare class TExecuteStatementReq {
  public sessionHandle: TSessionHandle;
  public statement: string;
  public confOverlay?: { [k: string]: string; };
  public runAsync?: boolean;
  public getDirectResults?: TSparkGetDirectResults;
  public queryTimeout?: Int64;
  public canReadArrowResult?: boolean;
  public canDownloadResult?: boolean;
  public canDecompressLZ4Result?: boolean;
  public maxBytesPerFile?: Int64;
  public useArrowNativeTypes?: TSparkArrowTypes;
  public resultRowLimit?: Int64;
  public parameters?: TSparkParameter[];
  public maxBytesPerBatch?: Int64;
  public statementConf?: TStatementConf;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;
  public rejectHighCostQueries?: boolean;
  public estimatedCost?: number;
  public executionVersion?: number;
  public requestValidation?: Buffer;
  public resultPersistenceMode?: TResultPersistenceMode;
  public trimArrowBatchesToLimit?: boolean;
  public fetchDisposition?: TDBSqlFetchDisposition;
  public enforceResultPersistenceMode?: boolean;
  public statementList?: TDBSqlStatement[];
  public persistResultManifest?: boolean;
  public resultRetentionSeconds?: Int64;
  public resultByteLimit?: Int64;
  public resultDataFormat?: TDBSqlResultFormat;
  public originatingClientIdentity?: string;
  public preferSingleFileResult?: boolean;
  public preferDriverOnlyUpload?: boolean;
  public enforceEmbeddedSchemaCorrectness?: boolean;
  public idempotencyToken?: string;

    constructor(args?: { sessionHandle: TSessionHandle; statement: string; confOverlay?: { [k: string]: string; }; runAsync?: boolean; getDirectResults?: TSparkGetDirectResults; queryTimeout?: Int64; canReadArrowResult?: boolean; canDownloadResult?: boolean; canDecompressLZ4Result?: boolean; maxBytesPerFile?: Int64; useArrowNativeTypes?: TSparkArrowTypes; resultRowLimit?: Int64; parameters?: TSparkParameter[]; maxBytesPerBatch?: Int64; statementConf?: TStatementConf; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; rejectHighCostQueries?: boolean; estimatedCost?: number; executionVersion?: number; requestValidation?: Buffer; resultPersistenceMode?: TResultPersistenceMode; trimArrowBatchesToLimit?: boolean; fetchDisposition?: TDBSqlFetchDisposition; enforceResultPersistenceMode?: boolean; statementList?: TDBSqlStatement[]; persistResultManifest?: boolean; resultRetentionSeconds?: Int64; resultByteLimit?: Int64; resultDataFormat?: TDBSqlResultFormat; originatingClientIdentity?: string; preferSingleFileResult?: boolean; preferDriverOnlyUpload?: boolean; enforceEmbeddedSchemaCorrectness?: boolean; idempotencyToken?: string; });
}

declare class TDBSqlStatement {
  public statement?: string;

    constructor(args?: { statement?: string; });
}

declare class TSparkParameterValue {
  public stringValue?: string;
  public doubleValue?: number;
  public booleanValue?: boolean;

    constructor(args?: { stringValue?: string; doubleValue?: number; booleanValue?: boolean; });
}

declare class TSparkParameter {
  public ordinal?: number;
  public name?: string;
  public type?: string;
  public value?: TSparkParameterValue;

    constructor(args?: { ordinal?: number; name?: string; type?: string; value?: TSparkParameterValue; });
}

declare class TStatementConf {
  public sessionless?: boolean;
  public initialNamespace?: TNamespace;
  public client_protocol?: TProtocolVersion;
  public client_protocol_i64?: Int64;

    constructor(args?: { sessionless?: boolean; initialNamespace?: TNamespace; client_protocol?: TProtocolVersion; client_protocol_i64?: Int64; });
}

declare class TExecuteStatementResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;
  public executionRejected?: boolean;
  public maxClusterCapacity?: number;
  public queryCost?: number;
  public sessionConf?: TDBSqlSessionConf;
  public currentClusterLoad?: number;
  public idempotencyType?: TOperationIdempotencyType;
  public remoteResultCacheEnabled?: boolean;
  public isServerless?: boolean;
  public operationHandles?: TOperationHandle[];

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; executionRejected?: boolean; maxClusterCapacity?: number; queryCost?: number; sessionConf?: TDBSqlSessionConf; currentClusterLoad?: number; idempotencyType?: TOperationIdempotencyType; remoteResultCacheEnabled?: boolean; isServerless?: boolean; operationHandles?: TOperationHandle[]; });
}

declare class TGetTypeInfoReq {
  public sessionHandle: TSessionHandle;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetTypeInfoResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetCatalogsReq {
  public sessionHandle: TSessionHandle;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetCatalogsResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetSchemasReq {
  public sessionHandle: TSessionHandle;
  public catalogName?: string;
  public schemaName?: string;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; catalogName?: string; schemaName?: string; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetSchemasResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetTablesReq {
  public sessionHandle: TSessionHandle;
  public catalogName?: string;
  public schemaName?: string;
  public tableName?: string;
  public tableTypes?: string[];
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; catalogName?: string; schemaName?: string; tableName?: string; tableTypes?: string[]; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetTablesResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetTableTypesReq {
  public sessionHandle: TSessionHandle;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetTableTypesResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetColumnsReq {
  public sessionHandle: TSessionHandle;
  public catalogName?: string;
  public schemaName?: string;
  public tableName?: string;
  public columnName?: string;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; catalogName?: string; schemaName?: string; tableName?: string; columnName?: string; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetColumnsResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetFunctionsReq {
  public sessionHandle: TSessionHandle;
  public catalogName?: string;
  public schemaName?: string;
  public functionName: string;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; catalogName?: string; schemaName?: string; functionName: string; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetFunctionsResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetPrimaryKeysReq {
  public sessionHandle: TSessionHandle;
  public catalogName?: string;
  public schemaName?: string;
  public tableName?: string;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; catalogName?: string; schemaName?: string; tableName?: string; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetPrimaryKeysResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetCrossReferenceReq {
  public sessionHandle: TSessionHandle;
  public parentCatalogName?: string;
  public parentSchemaName?: string;
  public parentTableName?: string;
  public foreignCatalogName?: string;
  public foreignSchemaName?: string;
  public foreignTableName?: string;
  public getDirectResults?: TSparkGetDirectResults;
  public runAsync?: boolean;
  public operationId?: THandleIdentifier;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; parentCatalogName?: string; parentSchemaName?: string; parentTableName?: string; foreignCatalogName?: string; foreignSchemaName?: string; foreignTableName?: string; getDirectResults?: TSparkGetDirectResults; runAsync?: boolean; operationId?: THandleIdentifier; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetCrossReferenceResp {
  public status: TStatus;
  public operationHandle?: TOperationHandle;
  public directResults?: TSparkDirectResults;

    constructor(args?: { status: TStatus; operationHandle?: TOperationHandle; directResults?: TSparkDirectResults; });
}

declare class TGetOperationStatusReq {
  public operationHandle: TOperationHandle;
  public getProgressUpdate?: boolean;

    constructor(args?: { operationHandle: TOperationHandle; getProgressUpdate?: boolean; });
}

declare class TGetOperationStatusResp {
  public status: TStatus;
  public operationState?: TOperationState;
  public sqlState?: string;
  public errorCode?: number;
  public errorMessage?: string;
  public taskStatus?: string;
  public operationStarted?: Int64;
  public operationCompleted?: Int64;
  public hasResultSet?: boolean;
  public progressUpdateResponse?: TProgressUpdateResp;
  public numModifiedRows?: Int64;
  public displayMessage?: string;
  public diagnosticInfo?: string;
  public errorDetailsJson?: string;
  public responseValidation?: Buffer;
  public idempotencyType?: TOperationIdempotencyType;
  public statementTimeout?: Int64;
  public statementTimeoutLevel?: TOperationTimeoutLevel;

    constructor(args?: { status: TStatus; operationState?: TOperationState; sqlState?: string; errorCode?: number; errorMessage?: string; taskStatus?: string; operationStarted?: Int64; operationCompleted?: Int64; hasResultSet?: boolean; progressUpdateResponse?: TProgressUpdateResp; numModifiedRows?: Int64; displayMessage?: string; diagnosticInfo?: string; errorDetailsJson?: string; responseValidation?: Buffer; idempotencyType?: TOperationIdempotencyType; statementTimeout?: Int64; statementTimeoutLevel?: TOperationTimeoutLevel; });
}

declare class TCancelOperationReq {
  public operationHandle: TOperationHandle;
  public executionVersion?: number;
  public replacedByNextAttempt?: boolean;

    constructor(args?: { operationHandle: TOperationHandle; executionVersion?: number; replacedByNextAttempt?: boolean; });
}

declare class TCancelOperationResp {
  public status: TStatus;

    constructor(args?: { status: TStatus; });
}

declare class TCloseOperationReq {
  public operationHandle: TOperationHandle;

    constructor(args?: { operationHandle: TOperationHandle; });
}

declare class TCloseOperationResp {
  public status: TStatus;

    constructor(args?: { status: TStatus; });
}

declare class TGetResultSetMetadataReq {
  public operationHandle: TOperationHandle;
  public includeCloudResultFiles?: boolean;

    constructor(args?: { operationHandle: TOperationHandle; includeCloudResultFiles?: boolean; });
}

declare class TGetResultSetMetadataResp {
  public status: TStatus;
  public schema?: TTableSchema;
  public resultFormat?: TSparkRowSetType;
  public lz4Compressed?: boolean;
  public arrowSchema?: Buffer;
  public cacheLookupResult?: TCacheLookupResult;
  public uncompressedBytes?: Int64;
  public compressedBytes?: Int64;
  public isStagingOperation?: boolean;
  public reasonForNoCloudFetch?: TCloudFetchDisabledReason;
  public resultFiles?: TDBSqlCloudResultFile[];
  public manifestFile?: string;
  public manifestFileFormat?: TDBSqlManifestFileFormat;
  public cacheLookupLatency?: Int64;
  public remoteCacheMissReason?: string;
  public fetchDisposition?: TDBSqlFetchDisposition;
  public remoteResultCacheEnabled?: boolean;
  public isServerless?: boolean;
  public resultDataFormat?: TDBSqlResultFormat;
  public truncatedByThriftLimit?: boolean;
  public resultByteLimit?: Int64;

    constructor(args?: { status: TStatus; schema?: TTableSchema; resultFormat?: TSparkRowSetType; lz4Compressed?: boolean; arrowSchema?: Buffer; cacheLookupResult?: TCacheLookupResult; uncompressedBytes?: Int64; compressedBytes?: Int64; isStagingOperation?: boolean; reasonForNoCloudFetch?: TCloudFetchDisabledReason; resultFiles?: TDBSqlCloudResultFile[]; manifestFile?: string; manifestFileFormat?: TDBSqlManifestFileFormat; cacheLookupLatency?: Int64; remoteCacheMissReason?: string; fetchDisposition?: TDBSqlFetchDisposition; remoteResultCacheEnabled?: boolean; isServerless?: boolean; resultDataFormat?: TDBSqlResultFormat; truncatedByThriftLimit?: boolean; resultByteLimit?: Int64; });
}

declare class TFetchResultsReq {
  public operationHandle: TOperationHandle;
  public orientation?: TFetchOrientation;
  public maxRows: Int64;
  public fetchType?: number;
  public maxBytes?: Int64;
  public startRowOffset?: Int64;
  public includeResultSetMetadata?: boolean;

    constructor(args?: { operationHandle: TOperationHandle; orientation?: TFetchOrientation; maxRows: Int64; fetchType?: number; maxBytes?: Int64; startRowOffset?: Int64; includeResultSetMetadata?: boolean; });
}

declare class TFetchResultsResp {
  public status: TStatus;
  public hasMoreRows?: boolean;
  public results?: TRowSet;
  public resultSetMetadata?: TGetResultSetMetadataResp;
  public responseValidation?: Buffer;

    constructor(args?: { status: TStatus; hasMoreRows?: boolean; results?: TRowSet; resultSetMetadata?: TGetResultSetMetadataResp; responseValidation?: Buffer; });
}

declare class TGetDelegationTokenReq {
  public sessionHandle: TSessionHandle;
  public owner: string;
  public renewer: string;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; owner: string; renewer: string; sessionConf?: TDBSqlSessionConf; });
}

declare class TGetDelegationTokenResp {
  public status: TStatus;
  public delegationToken?: string;

    constructor(args?: { status: TStatus; delegationToken?: string; });
}

declare class TCancelDelegationTokenReq {
  public sessionHandle: TSessionHandle;
  public delegationToken: string;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; delegationToken: string; sessionConf?: TDBSqlSessionConf; });
}

declare class TCancelDelegationTokenResp {
  public status: TStatus;

    constructor(args?: { status: TStatus; });
}

declare class TRenewDelegationTokenReq {
  public sessionHandle: TSessionHandle;
  public delegationToken: string;
  public sessionConf?: TDBSqlSessionConf;

    constructor(args?: { sessionHandle: TSessionHandle; delegationToken: string; sessionConf?: TDBSqlSessionConf; });
}

declare class TRenewDelegationTokenResp {
  public status: TStatus;

    constructor(args?: { status: TStatus; });
}

declare class TProgressUpdateResp {
  public headerNames: string[];
  public rows: string[][];
  public progressedPercentage: number;
  public status: TJobExecutionStatus;
  public footerSummary: string;
  public startTime: Int64;

    constructor(args?: { headerNames: string[]; rows: string[][]; progressedPercentage: number; status: TJobExecutionStatus; footerSummary: string; startTime: Int64; });
}

declare var PRIMITIVE_TYPES: TTypeId[];

declare var COMPLEX_TYPES: TTypeId[];

declare var COLLECTION_TYPES: TTypeId[];

declare var TYPE_NAMES: { [k: number /*TTypeId*/]: string; };

declare var CHARACTER_MAXIMUM_LENGTH: string;

declare var PRECISION: string;

declare var SCALE: string;
