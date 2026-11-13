/**
  This file is created using node_modules/thrift/lib/nodejs/lib/thrift/http_connection.js as an example

  The code relies on thrift internals, so be careful when upgrading `thrift` library
*/

import { EventEmitter } from 'events';
import { TBinaryProtocol, TBufferedTransport, Thrift, TProtocol, TProtocolConstructor, TTransport } from 'thrift';
import fetch, { RequestInit, HeadersInit, Request, Response, FetchError } from 'node-fetch';
// @ts-expect-error TS7016: Could not find a declaration file for module
import InputBufferUnderrunError from 'thrift/lib/nodejs/lib/thrift/input_buffer_underrun_error';
import IRetryPolicy from '../contracts/IRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import NullRetryPolicy from './NullRetryPolicy';

export class THTTPException extends Thrift.TApplicationException {
  public readonly statusCode: unknown;

  public readonly response: Response;

  constructor(response: Response) {
    const detail = getErrorDetailFromHeaders(response);
    const message = `Received a response with a bad HTTP status code: ${response.status}`;
    super(Thrift.TApplicationExceptionType.PROTOCOL_ERROR, detail ? `${message}: ${detail}` : message);
    this.statusCode = response.status;
    this.response = response;
  }
}

// Server error headers, most specific first.
const errorDetailHeaders = [
  'x-thriftserver-error-message',
  'x-databricks-error-or-redirect-message',
  'x-databricks-reason-phrase',
];

const MAX_ERROR_DETAIL_LENGTH = 500;

function getErrorDetailFromHeaders(response: Response): string | undefined {
  for (const header of errorDetailHeaders) {
    const detail = (response.headers.get(header) ?? '').replace(/\s+/g, ' ').trim();
    if (detail) {
      return detail.length > MAX_ERROR_DETAIL_LENGTH ? `${detail.slice(0, MAX_ERROR_DETAIL_LENGTH - 1)}…` : detail;
    }
  }
  return undefined;
}

// thrift >= 0.25 accepts an optional `maxLength` that @types/thrift doesn't declare.
type TTransportType = typeof TBufferedTransport & {
  receiver(
    callback: (trans: TBufferedTransport, seqid: number) => void,
    seqid: number,
    maxLength?: number,
  ): (data: Buffer) => void;
};

interface ThriftHttpConnectionOptions {
  url: string;
  transport?: TTransportType;
  protocol?: TProtocolConstructor;
  getRetryPolicy(): Promise<IRetryPolicy<HttpTransactionDetails>>;
  getNonIdempotentRetryPolicy?(): Promise<IRetryPolicy<HttpTransactionDetails>>;
}

// This type describes a shape of internals of Thrift client object.
// It is not perfect good enough for our needs
type ThriftClient = {
  // Internal map of callbacks of running requests. Once request is completed (either successfully or not) -
  // callback should be removed from it
  _reqs: Record<number, (error: unknown, response?: unknown) => void>;
} & {
  // For each client's public method Foo there are two private ones: send_Foo and recv_Foo.
  // We have to access recv_Foo ones to properly parse the response
  [key: string]: (input: TProtocol, mtype: Thrift.MessageType, seqId: number) => void;
};

const retryableThriftMethods = new Set([
  // A repeated OpenSession leaves at most an unused session, which the server expires
  'OpenSession',
  'GetOperationStatus',
  'CancelOperation',
  'CloseOperation',
  'GetResultSetMetadata',
  'CloseSession',
  'GetInfo',
  'GetTypeInfo',
  'GetCatalogs',
  'GetSchemas',
  'GetTables',
  'GetTableTypes',
  'GetColumns',
  'GetFunctions',
  'GetPrimaryKeys',
  'GetCrossReference',
]);

// Not safe to repeat once the server may have run the request, so retried only when it did not
const nonIdempotentThriftMethods = new Set(['ExecuteStatement']);

export default class ThriftHttpConnection extends EventEmitter {
  private readonly url: string;

  private config: RequestInit;

  private options: ThriftHttpConnectionOptions;

  // This field is used by Thrift internally, so name and type are important
  private readonly transport: TTransportType;

  // This field is used by Thrift internally, so name and type are important
  private readonly protocol: TProtocolConstructor;

  // thrift.createClient sets this field internally
  public client?: ThriftClient;

  constructor(options: ThriftHttpConnectionOptions, config: RequestInit = {}) {
    super();
    this.url = options.url;
    this.config = config;
    this.options = options;
    this.transport = options.transport ?? TBufferedTransport;
    this.protocol = options.protocol ?? TBinaryProtocol;
  }

  protected async getRetryPolicy(thriftMethodName?: string): Promise<IRetryPolicy<HttpTransactionDetails>> {
    // Allow retry behavior only for Thrift operations that are for sure safe to retry
    if (thriftMethodName && retryableThriftMethods.has(thriftMethodName)) {
      return this.options.getRetryPolicy();
    }
    if (
      thriftMethodName &&
      nonIdempotentThriftMethods.has(thriftMethodName) &&
      this.options.getNonIdempotentRetryPolicy
    ) {
      return this.options.getNonIdempotentRetryPolicy();
    }
    // Don't retry everything that is not explicitly allowed to retry
    return new NullRetryPolicy();
  }

  public setHeaders(headers: HeadersInit) {
    this.config = {
      ...this.config,
      headers,
    };
  }

  public write(data: Buffer, seqId: number) {
    const requestConfig: RequestInit = {
      ...this.config,
      method: 'POST',
      headers: {
        ...this.config.headers,
        Connection: 'keep-alive',
        'Content-Length': `${data.length}`,
        'Content-Type': 'application/x-thrift',
      },
      body: data,
    };

    // Consume the response body inside the retried block. node-fetch surfaces
    // late-stage failures (TCP RST after headers, "Premature close") as rejections
    // from `response.buffer()`, not from `fetch()`. Reading the body here means
    // the retry policy sees those failures and can retry them like any other
    // transient network error — the body Buffer is captured via closure so the
    // post-retry caller still gets it.
    let responseBuffer: Buffer | undefined;

    this.getThriftMethodName(data)
      .then((thriftMethod) => this.getRetryPolicy(thriftMethod))
      .then((retryPolicy) => {
        const makeRequest = async () => {
          responseBuffer = undefined;
          const request = new Request(this.url, requestConfig);
          const response = await fetch(request);
          if (response.status === 200) {
            responseBuffer = await response.buffer();
          }
          return { request, response };
        };
        return retryPolicy.invokeWithRetry(makeRequest);
      })
      .then(({ response }) => {
        if (response.status !== 200) {
          throw new THTTPException(response);
        }
        // `responseBuffer` is always set when status is 200, since `makeRequest`
        // assigns it before resolving in that branch and the retry loop only
        // returns a fulfilled response (failures are thrown).
        return responseBuffer as Buffer;
      })
      .then((buffer) => {
        // The whole body is already in memory. Capping at its size stops thrift >= 0.25 from rounding the
        // receive buffer up to a power of two, which is much slower for multi-MB responses.
        this.transport.receiver(
          (transportWithData) => this.handleThriftResponse(transportWithData),
          seqId,
          buffer.length,
        )(buffer);
      })
      .catch((error) => {
        if (error instanceof FetchError) {
          if (error.type === 'request-timeout') {
            error = new Thrift.TApplicationException(
              Thrift.TApplicationExceptionType.PROTOCOL_ERROR,
              'Request timed out',
            );
          }
        }

        const defaultErrorHandler = (err: unknown) => {
          this.emit('error', err);
        };

        if (this.client) {
          const callback = this.client._reqs[seqId] ?? defaultErrorHandler;
          delete this.client._reqs[seqId];
          callback(error);
        } else {
          defaultErrorHandler(error);
        }
      });
  }

  private getThriftMethodName(thriftMessage: Buffer): Promise<string | undefined> {
    return new Promise((resolve) => {
      try {
        // Cap at the message size, as for responses, so thrift >= 0.25 doesn't copy every outgoing request
        // into a buffer rounded up to a power of two.
        const receiver = this.transport.receiver(
          (transportWithData) => {
            const Protocol = this.protocol;
            const proto = new Protocol(transportWithData);
            const header = proto.readMessageBegin();
            resolve(header.fname);
          },
          0 /* `seqId` could be any because it's ignored */,
          thriftMessage.length,
        );

        receiver(thriftMessage);
      } catch {
        resolve(undefined);
      }
    });
  }

  private handleThriftResponse(transportWithData: TTransport) {
    if (!this.client) {
      throw new Thrift.TApplicationException(Thrift.TApplicationExceptionType.INTERNAL_ERROR, 'Client not available');
    }

    const Protocol = this.protocol;
    const proto = new Protocol(transportWithData);
    try {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const header = proto.readMessageBegin();
        const dummySeqId = header.rseqid * -1;
        const { client } = this;

        client._reqs[dummySeqId] = (err, success) => {
          transportWithData.commitPosition();
          const clientCallback = client._reqs[header.rseqid];
          delete client._reqs[header.rseqid];
          if (clientCallback) {
            process.nextTick(() => {
              clientCallback(err, success);
            });
          }
        };

        if (client[`recv_${header.fname}`]) {
          client[`recv_${header.fname}`](proto, header.mtype, dummySeqId);
        } else {
          delete client._reqs[dummySeqId];
          throw new Thrift.TApplicationException(
            Thrift.TApplicationExceptionType.WRONG_METHOD_NAME,
            'Received a response to an unknown RPC function',
          );
        }
      }
    } catch (error) {
      if (error instanceof InputBufferUnderrunError) {
        transportWithData.rollbackPosition();
      } else {
        throw error;
      }
    }
  }
}
