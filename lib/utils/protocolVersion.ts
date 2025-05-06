import { TProtocolVersion } from '../../thrift/TCLIService_types';

/**
 * Protocol version information from Thrift TCLIService
 * Each version adds certain features to the Spark/Hive API
 * 
 * Databricks only supports SPARK_CLI_SERVICE_PROTOCOL_V1 (0xA501) or higher
 */

/**
 * Check if the current protocol version supports a specific feature
 * @param serverProtocolVersion The protocol version received from server in TOpenSessionResp
 * @param requiredVersion The minimum protocol version required for a feature
 * @returns boolean indicating if the feature is supported
 */
export function isFeatureSupported(
  serverProtocolVersion: TProtocolVersion | undefined | null,
  requiredVersion: TProtocolVersion
): boolean {
  if (serverProtocolVersion === undefined || serverProtocolVersion === null) {
    return false;
  }
  
  return serverProtocolVersion >= requiredVersion;
}

/**
 * Check if parameterized queries are supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V8 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if parameterized queries are supported
 */
export function supportsParameterizedQueries(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8);
}

/**
 * Check if async metadata operations are supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V6 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if async metadata operations are supported
 */
export function supportsAsyncMetadataOperations(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6);
}

/**
 * Check if result persistence mode is supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V7 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if result persistence mode is supported
 */
export function supportsResultPersistenceMode(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7);
}

/**
 * Check if Arrow compression is supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V6 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if compressed Arrow batches are supported
 */
export function supportsArrowCompression(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6);
}

/**
 * Check if Arrow metadata is supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V5 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if Arrow metadata is supported
 */
export function supportsArrowMetadata(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5);
}

/**
 * Check if multiple catalogs are supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V4 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if multiple catalogs are supported
 */
export function supportsMultipleCatalogs(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4);
}

/**
 * Check if cloud object storage fetching is supported
 * (Requires SPARK_CLI_SERVICE_PROTOCOL_V3 or higher)
 * @param serverProtocolVersion The protocol version from server
 * @returns boolean indicating if cloud fetching is supported
 */
export function supportsCloudFetch(
  serverProtocolVersion: TProtocolVersion | undefined | null
): boolean {
  return isFeatureSupported(serverProtocolVersion, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3);
}