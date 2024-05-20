import { expect } from 'chai';
import { TStatusCode } from '../../../thrift/TCLIService_types';
import ArrowResultHandler from '../../../lib/result/ArrowResultHandler';
import ArrowResultConverter from '../../../lib/result/ArrowResultConverter';
import JsonResultHandler from '../../../lib/result/JsonResultHandler';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';

import ClientContextStub from '../.stubs/ClientContextStub';

import { fixArrowResult } from '../../fixtures/compatibility';
import * as fixtureColumn from '../../fixtures/compatibility/column';
import * as fixtureArrow from '../../fixtures/compatibility/arrow';
import * as fixtureArrowNT from '../../fixtures/compatibility/arrow_native_types';

describe('Result handlers compatibility tests', () => {
  it('colum-based data', async () => {
    const context = new ClientContextStub();
    const rowSetProvider = new ResultsProviderStub(fixtureColumn.rowSets, undefined);
    const result = new JsonResultHandler(context, rowSetProvider, {
      schema: fixtureColumn.schema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    const rows = await result.fetchNext({ limit: 10000 });
    expect(rows).to.deep.equal(fixtureColumn.expected);
  });

  it('arrow-based data without native types', async () => {
    const context = new ClientContextStub();
    const rowSetProvider = new ResultsProviderStub(fixtureArrow.rowSets, undefined);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, {
        arrowSchema: fixtureArrow.arrowSchema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      }),
      { schema: fixtureArrow.schema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });

  it('arrow-based data with native types', async () => {
    const context = new ClientContextStub();
    const rowSetProvider = new ResultsProviderStub(fixtureArrowNT.rowSets, undefined);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, {
        arrowSchema: fixtureArrowNT.arrowSchema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      }),
      { schema: fixtureArrowNT.schema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrowNT.expected);
  });

  it('should infer arrow schema from thrift schema', async () => {
    const context = new ClientContextStub();
    const rowSetProvider = new ResultsProviderStub(fixtureArrow.rowSets, undefined);
    const result = new ArrowResultConverter(
      context,
      new ArrowResultHandler(context, rowSetProvider, {
        schema: fixtureArrow.schema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      }),
      { schema: fixtureArrow.schema, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
    );
    const rows = await result.fetchNext({ limit: 10000 });
    expect(fixArrowResult(rows)).to.deep.equal(fixtureArrow.expected);
  });
});
