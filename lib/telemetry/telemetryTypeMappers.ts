/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TOperationType, TSparkRowSetType } from '../../thrift/TCLIService_types';

/**
 * Map Thrift TOperationType to telemetry Operation.Type enum string.
 */
export function mapOperationTypeToTelemetryType(operationType?: TOperationType): string | undefined {
  if (operationType === undefined) {
    return undefined;
  }

  switch (operationType) {
    case TOperationType.EXECUTE_STATEMENT:
      return 'EXECUTE_STATEMENT';
    case TOperationType.GET_TYPE_INFO:
      return 'LIST_TYPE_INFO';
    case TOperationType.GET_CATALOGS:
      return 'LIST_CATALOGS';
    case TOperationType.GET_SCHEMAS:
      return 'LIST_SCHEMAS';
    case TOperationType.GET_TABLES:
      return 'LIST_TABLES';
    case TOperationType.GET_TABLE_TYPES:
      return 'LIST_TABLE_TYPES';
    case TOperationType.GET_COLUMNS:
      return 'LIST_COLUMNS';
    case TOperationType.GET_FUNCTIONS:
      return 'LIST_FUNCTIONS';
    case TOperationType.UNKNOWN:
    default:
      return 'TYPE_UNSPECIFIED';
  }
}

/**
 * Map Thrift TSparkRowSetType to telemetry ExecutionResult.Format enum string.
 */
export function mapResultFormatToTelemetryType(resultFormat?: TSparkRowSetType): string | undefined {
  if (resultFormat === undefined) {
    return undefined;
  }

  switch (resultFormat) {
    case TSparkRowSetType.ARROW_BASED_SET:
      return 'INLINE_ARROW';
    case TSparkRowSetType.COLUMN_BASED_SET:
      return 'COLUMNAR_INLINE';
    case TSparkRowSetType.ROW_BASED_SET:
      return 'INLINE_JSON';
    case TSparkRowSetType.URL_BASED_SET:
      return 'EXTERNAL_LINKS';
    default:
      return 'FORMAT_UNSPECIFIED';
  }
}
