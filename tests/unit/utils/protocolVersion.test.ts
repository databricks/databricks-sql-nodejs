import { expect } from 'chai';
import { TProtocolVersion } from '../../../thrift/TCLIService_types';
import * as ProtocolVersion from '../../../lib/utils/protocolVersion';

describe('Protocol Version Utility - Parameterized Tests', () => {
  // Define minimum protocol versions for each feature
  const MIN_VERSION_CLOUD_FETCH = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3;
  const MIN_VERSION_MULTIPLE_CATALOGS = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4;
  const MIN_VERSION_ARROW_METADATA = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5;
  const MIN_VERSION_ARROW_COMPRESSION = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6;
  const MIN_VERSION_ASYNC_METADATA = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6;
  const MIN_VERSION_RESULT_PERSISTENCE = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7;
  const MIN_VERSION_PARAMETERIZED = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8;

  // Create an array of all protocol versions to test against
  const protocolVersions = [
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V2,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
    TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
  ];

  // Test each protocol version against each feature function
  protocolVersions.forEach((version) => {
    describe(`with protocol version ${version}`, () => {
      it('supportsCloudFetch', () => {
        const expected = version >= MIN_VERSION_CLOUD_FETCH;
        const actual = ProtocolVersion.supportsCloudFetch(version);
        expect(actual).to.equal(expected);
      });

      it('supportsMultipleCatalogs', () => {
        const expected = version >= MIN_VERSION_MULTIPLE_CATALOGS;
        const actual = ProtocolVersion.supportsMultipleCatalogs(version);
        expect(actual).to.equal(expected);
      });

      it('supportsArrowMetadata', () => {
        const expected = version >= MIN_VERSION_ARROW_METADATA;
        const actual = ProtocolVersion.supportsArrowMetadata(version);
        expect(actual).to.equal(expected);
      });

      it('supportsArrowCompression', () => {
        const expected = version >= MIN_VERSION_ARROW_COMPRESSION;
        const actual = ProtocolVersion.supportsArrowCompression(version);
        expect(actual).to.equal(expected);
      });

      it('supportsAsyncMetadataOperations', () => {
        const expected = version >= MIN_VERSION_ASYNC_METADATA;
        const actual = ProtocolVersion.supportsAsyncMetadataOperations(version);
        expect(actual).to.equal(expected);
      });

      it('supportsResultPersistenceMode', () => {
        const expected = version >= MIN_VERSION_RESULT_PERSISTENCE;
        const actual = ProtocolVersion.supportsResultPersistenceMode(version);
        expect(actual).to.equal(expected);
      });

      it('supportsParameterizedQueries', () => {
        const expected = version >= MIN_VERSION_PARAMETERIZED;
        const actual = ProtocolVersion.supportsParameterizedQueries(version);
        expect(actual).to.equal(expected);
      });
    });
  });
});
