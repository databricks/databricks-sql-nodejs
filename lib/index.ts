// Don't move this import - it should be placed before any other
import './polyfills';

import { Thrift } from 'thrift';
import TCLIService from '../thrift/TCLIService';
import TCLIService_types from '../thrift/TCLIService_types';
import DBSQLClient from './DBSQLClient';
import DBSQLSession from './DBSQLSession';
import { DBSQLParameter, DBSQLParameterType } from './DBSQLParameter';
import DBSQLLogger from './DBSQLLogger';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import {
  Token,
  StaticTokenProvider,
  ExternalTokenProvider,
  CachedTokenProvider,
  FederationProvider,
} from './connection/auth/tokenProvider';
import HttpConnection from './connection/connections/HttpConnection';
import { formatProgress } from './utils';
import { LogLevel } from './contracts/IDBSQLLogger';

// Re-export types for TypeScript users
export type { default as ITokenProvider } from './connection/auth/tokenProvider/ITokenProvider';

export { CircuitBreakerOpenError, CIRCUIT_BREAKER_OPEN_CODE } from './telemetry/CircuitBreaker';
export { TelemetryTerminalError } from './telemetry/DatabricksTelemetryExporter';

// Telemetry event/metric/config shapes for consumers that want to inspect
// telemetry payloads or pre-validate config. The emitter, aggregator, and
// per-host client are deliberately not re-exported — they are internal.
export { TelemetryEventType, DEFAULT_TELEMETRY_CONFIG } from './telemetry/types';
export type {
  TelemetryEvent,
  TelemetryMetric,
  TelemetryConfiguration,
  StatementMetrics,
  DriverConfiguration,
} from './telemetry/types';

export const auth = {
  PlainHttpAuthentication,
  // Token provider classes for custom authentication
  Token,
  StaticTokenProvider,
  ExternalTokenProvider,
  CachedTokenProvider,
  FederationProvider,
};

const { TException, TApplicationException, TApplicationExceptionType, TProtocolException, TProtocolExceptionType } =
  Thrift;

export { TException, TApplicationException, TApplicationExceptionType, TProtocolException, TProtocolExceptionType };

export const connections = {
  HttpConnection,
};

export const thrift = {
  TCLIService,
  TCLIService_types,
};

export const utils = {
  formatProgress,
};

export { DBSQLClient, DBSQLSession, DBSQLParameter, DBSQLParameterType, DBSQLLogger, LogLevel };
