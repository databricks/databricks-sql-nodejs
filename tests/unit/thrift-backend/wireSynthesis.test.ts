import { expect } from 'chai';
import { TOperationState, TSparkRowSetType, TStatusCode } from '../../../thrift/TCLIService_types';
import { OperationState, OperationStatus } from '../../../lib/contracts/OperationStatus';
import { ResultFormat, ResultMetadata } from '../../../lib/contracts/ResultMetadata';
import { synthesizeThriftStatus, synthesizeThriftResultSetMetadata } from '../../../lib/thrift-backend/wireSynthesis';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

describe('wireSynthesis', () => {
  describe('synthesizeThriftStatus', () => {
    const baseStatus: OperationStatus = { state: OperationState.Succeeded };

    it('maps each OperationState to the right TOperationState', () => {
      const cases: Array<[OperationState, TOperationState]> = [
        [OperationState.Pending, TOperationState.PENDING_STATE],
        [OperationState.Running, TOperationState.RUNNING_STATE],
        [OperationState.Succeeded, TOperationState.FINISHED_STATE],
        [OperationState.Failed, TOperationState.ERROR_STATE],
        [OperationState.Cancelled, TOperationState.CANCELED_STATE],
        [OperationState.Closed, TOperationState.CLOSED_STATE],
        [OperationState.Unknown, TOperationState.UKNOWN_STATE],
      ];
      for (const [state, expected] of cases) {
        const resp = synthesizeThriftStatus({ state });
        expect(resp.operationState, `state=${state}`).to.equal(expected);
      }
    });

    it('returns SUCCESS_STATUS for non-terminal states', () => {
      for (const state of [OperationState.Pending, OperationState.Running, OperationState.Succeeded]) {
        const resp = synthesizeThriftStatus({ state });
        expect(resp.status.statusCode, `state=${state}`).to.equal(TStatusCode.SUCCESS_STATUS);
      }
    });

    it('returns ERROR_STATUS for Failed and carries errorMessage + sqlState', () => {
      const resp = synthesizeThriftStatus({
        state: OperationState.Failed,
        errorMessage: 'boom',
        sqlState: '42000',
      });
      expect(resp.status.statusCode).to.equal(TStatusCode.ERROR_STATUS);
      expect(resp.status.errorMessage).to.equal('boom');
      expect(resp.status.sqlState).to.equal('42000');
    });

    it('returns ERROR_STATUS for Cancelled', () => {
      const resp = synthesizeThriftStatus({ state: OperationState.Cancelled });
      expect(resp.status.statusCode).to.equal(TStatusCode.ERROR_STATUS);
    });

    it('returns ERROR_STATUS for Closed', () => {
      const resp = synthesizeThriftStatus({ state: OperationState.Closed });
      expect(resp.status.statusCode).to.equal(TStatusCode.ERROR_STATUS);
    });

    it('round-trips hasResultSet', () => {
      expect(synthesizeThriftStatus({ ...baseStatus, hasResultSet: true }).hasResultSet).to.equal(true);
      expect(synthesizeThriftStatus({ ...baseStatus, hasResultSet: false }).hasResultSet).to.equal(false);
      expect(synthesizeThriftStatus({ ...baseStatus }).hasResultSet).to.equal(undefined);
    });

    it('forwards errorMessage and sqlState on the top-level event payload', () => {
      const resp = synthesizeThriftStatus({
        state: OperationState.Succeeded,
        errorMessage: 'should-not-elevate-to-error-but-still-passed-through',
        sqlState: '01000',
      });
      expect(resp.errorMessage).to.equal('should-not-elevate-to-error-but-still-passed-through');
      expect(resp.sqlState).to.equal('01000');
    });
  });

  describe('synthesizeThriftResultSetMetadata', () => {
    const base: ResultMetadata = {
      resultFormat: ResultFormat.ColumnBased,
      isStagingOperation: false,
    };

    it('maps each ResultFormat to the right TSparkRowSetType', () => {
      expect(
        synthesizeThriftResultSetMetadata({ ...base, resultFormat: ResultFormat.ColumnBased }).resultFormat,
      ).to.equal(TSparkRowSetType.COLUMN_BASED_SET);
      expect(
        synthesizeThriftResultSetMetadata({ ...base, resultFormat: ResultFormat.ArrowBased }).resultFormat,
      ).to.equal(TSparkRowSetType.ARROW_BASED_SET);
      expect(synthesizeThriftResultSetMetadata({ ...base, resultFormat: ResultFormat.UrlBased }).resultFormat).to.equal(
        TSparkRowSetType.URL_BASED_SET,
      );
    });

    it('throws HiveDriverError on an unknown ResultFormat instead of silently aliasing', () => {
      expect(() =>
        synthesizeThriftResultSetMetadata({ ...base, resultFormat: 'BOGUS' as unknown as ResultFormat }),
      ).to.throw(HiveDriverError);
    });

    it('round-trips schema, arrowSchema, lz4Compressed and isStagingOperation', () => {
      const schema = { columns: [] } as ResultMetadata['schema'];
      const arrowSchema = Buffer.from([1, 2, 3]);
      const resp = synthesizeThriftResultSetMetadata({
        resultFormat: ResultFormat.ArrowBased,
        schema,
        arrowSchema,
        lz4Compressed: true,
        isStagingOperation: true,
      });
      expect(resp.schema).to.equal(schema);
      expect(resp.arrowSchema).to.equal(arrowSchema);
      expect(resp.lz4Compressed).to.equal(true);
      expect(resp.isStagingOperation).to.equal(true);
    });

    it('synthetic status is SUCCESS_STATUS — the RPC-success bit, not the operation outcome', () => {
      const resp = synthesizeThriftResultSetMetadata(base);
      expect(resp.status.statusCode).to.equal(TStatusCode.SUCCESS_STATUS);
    });
  });
});
