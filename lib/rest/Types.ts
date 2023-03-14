export enum Disposition {
  ExternalLinks = 'EXTERNAL_LINKS',
  Inline = 'INLINE',
}

export enum Format {
  ArrowStream = 'ARROW_STREAM',
  JsonArray = 'JSON_ARRAY',
}

export enum TimeoutAction {
  Cancel = 'CANCEL',
  Continue = 'CONTINUE',
}

export interface ExecuteStatementRequest {
  catalog?: string;
  disposition: Disposition;
  format: Format;
  on_wait_timeout: TimeoutAction;
  schema?: string;
  statement: string;
  wait_timeout: string;
  warehouse_id: string;
}

export interface ExecuteStatementResponse {
  manifest?: ResultManifest;
  result?: ResultData;
  statement_id: string;
  status: StatementStatus;
}

export enum StatementState {
  Canceled = 'CANCELED',
  Closed = 'CLOSED',
  Failed = 'FAILED',
  Pending = 'PENDING',
  Running = 'RUNNING',
  Succeeded = 'SUCCEEDED',
}

export interface StatementStatus {
  error: ServiceError;
  state: StatementState;
}

export interface ServiceError {
  error_code: ServiceErrorCode;
  message: string;
}

export enum ServiceErrorCode {
  Aborted = 'ABORTED',
  AlreadyExists = 'ALREADY_EXISTS',
  BadRequest = 'BAD_REQUEST',
  Cancelled = 'CANCELLED',
  DeadlineExceeded = 'DEADLINE_EXCEEDED',
  InternalError = 'INTERNAL_ERROR',
  IoError = 'IO_ERROR',
  NotFound = 'NOT_FOUND',
  ResourceExhausted = 'RESOURCE_EXHAUSTED',
  ServiceUnderMaintenance = 'SERVICE_UNDER_MAINTENANCE',
  TemporarilyUnavailable = 'TEMPORARILY_UNAVAILABLE',
  Unauthenticated = 'UNAUTHENTICATED',
  Unknown = 'UNKNOWN',
  WorkspaceTemporarilyUnavailable = 'WORKSPACE_TEMPORARILY_UNAVAILABLE',
}

export interface ResultData {
  byte_count: number; // int64
  chunk_index: number;
  data_array: string[][];
  external_links?: ExternalLink[];
  next_chunk_index?: number;
  next_chunk_internal_link?: string;
  row_count: number; // int64
  row_offset: number; // int64
}

interface ExternalLink {
  byte_count: number; // int64
  chunk_index: number;
  expiration: string;
  external_link: string;
  next_chunk_index: number;
  next_chunk_internal_link: string;
  row_count: number; // int64
  row_offset: number; // int64
}

export interface ResultManifest {
  chunks: ChunkInfo[];
  format: Format;
  schema: ResultSchema;
  total_byte_count: number; // int64
  total_chunk_count: number;
  total_row_count: number; // int64
}

export interface ChunkInfo {
  byte_count: number; // int64
  chunk_index: number;
  next_chunk_index: number;
  next_chunk_internal_link: string;
  row_count: number; // int64
  row_offset: number; // int64
}

export interface ResultSchema {
  column_count: number;
  columns: ColumnInfo[];
}

export interface ColumnInfo {
  name: string;
  position: number;
  type_interval_type: string;
  type_name: ColumnInfoTypeName;
  type_precision: number;
  type_scale: number;
  type_text: string;
}

export enum ColumnInfoTypeName {
  Array = 'ARRAY',
  Binary = 'BINARY',
  Boolean = 'BOOLEAN',
  Byte = 'BYTE',
  Char = 'CHAR',
  Date = 'DATE',
  Decimal = 'DECIMAL',
  Double = 'DOUBLE',
  Float = 'FLOAT',
  Int = 'INT',
  Interval = 'INTERVAL',
  Long = 'LONG',
  Map = 'MAP',
  Null = 'NULL',
  Short = 'SHORT',
  String = 'STRING',
  Struct = 'STRUCT',
  Timestamp = 'TIMESTAMP',
  UserDefinedType = 'USER_DEFINED_TYPE',
}

export interface CancelExecutionRequest {
  statement_id: string;
}

export interface GetStatementRequest {
  statement_id: string;
}

export interface GetStatementResponse {
  manifest: ResultManifest;
  result: ResultData;
  statement_id: string;
  status: StatementStatus;
}

export interface GetStatementResultChunkNRequest {
  chunk_index: number;
  statement_id: string;
}
