/**
  This file is created using node_modules/thrift/lib/nodejs/lib/thrift/http_connection.js as an example

  The code relies on thrift internals, so be careful when upgrading `thrift` library
*/

import { EventEmitter } from 'events';
import { PassThrough, pipeline } from 'stream';
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

  constructor(response: Response, detail?: string) {
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
const MAX_ERROR_BODY_SCAN = 4096;
// Budgets for reading an error body; when either runs out the error carries the status only
const MAX_ERROR_BODY_BYTES = 64 * 1024;
const MAX_ERROR_BODY_READ_MS = 1000;

function normalizeErrorDetail(value: string): string | undefined {
  const text = value.slice(0, MAX_ERROR_BODY_SCAN).replace(/\s+/g, ' ').trim();
  if (text.length === 0) {
    return undefined;
  }
  return text.length > MAX_ERROR_DETAIL_LENGTH ? `${text.slice(0, MAX_ERROR_DETAIL_LENGTH)}…` : text;
}

function getErrorDetailFromBody(body: Buffer): string | undefined {
  const text = body.toString('utf8', 0, MAX_ERROR_BODY_SCAN).trim();
  // Skip HTML pages and binary (e.g. Thrift-encoded) payloads
  // eslint-disable-next-line no-control-regex
  if (text.startsWith('<') || /[\u0000-\u0008\u000e-\u001f]/.test(text)) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(text);
    if (typeof parsed?.message === 'string') {
      return normalizeErrorDetail(parsed.message);
    }
  } catch {
    // Not JSON - use the text as is
  }
  return normalizeErrorDetail(text);
}

interface ErrorBodyReadResult {
  chunks: Buffer[];
  // `false` when the byte or time budget ran out before the body ended
  complete: boolean;
}

// Reads the error body until it ends, it exceeds `MAX_ERROR_BODY_BYTES`, or
// `MAX_ERROR_BODY_READ_MS` elapses, whichever comes first. The stream is left
// paused (not destroyed) when a budget runs out, so the rest stays readable.
function readErrorBody(body: NodeJS.ReadableStream): Promise<ErrorBodyReadResult> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let timer: NodeJS.Timeout | undefined;

    const cleanup = () => {
      clearTimeout(timer);
      body.removeListener('data', onData);
      body.removeListener('end', onEnd);
      body.removeListener('error', onError);
    };
    const stop = () => {
      cleanup();
      body.pause();
      resolve({ chunks, complete: false });
    };
    function onData(chunk: Buffer | string) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      chunks.push(buffer);
      size += buffer.length;
      if (size > MAX_ERROR_BODY_BYTES) {
        stop();
      }
    }
    function onEnd() {
      cleanup();
      resolve({ chunks, complete: true });
    }
    function onError(error: unknown) {
      cleanup();
      reject(error);
    }

    timer = setTimeout(stop, MAX_ERROR_BODY_READ_MS);
    body.on('data', onData);
    body.once('end', onEnd);
    body.once('error', onError);
  });
}

// Builds a response equivalent to `response` whose body is the `body` stream. The body is
// always a stream (as on a fetched response), so `.body.pipe()` / `.body.on()` keep working,
// and the original read limits (`timeout`, `size`) still apply to `text()` / `json()` etc.
function replayResponse(response: Response, body: PassThrough): Response {
  return new Response(body, {
    url: response.url,
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
    size: response.size,
    timeout: response.timeout,
  });
}

async function createHTTPException(response: Response): Promise<THTTPException> {
  for (const header of errorDetailHeaders) {
    const detail = normalizeErrorDetail(response.headers.get(header) ?? '');
    if (detail) {
      return new THTTPException(response, detail);
    }
  }

  // Don't start reading a body that is already known to exceed the byte budget
  const contentLength = Number(response.headers.get('content-length'));
  if (Number.isFinite(contentLength) && contentLength > MAX_ERROR_BODY_BYTES) {
    return new THTTPException(response);
  }

  let result: ErrorBodyReadResult;
  try {
    result = await readErrorBody(response.body);
  } catch {
    return new THTTPException(response);
  }

  const prefix = Buffer.concat(result.chunks);
  const replay = new PassThrough();

  if (!result.complete) {
    // Budget exhausted: report the status only, and expose a response that still
    // yields the whole body (the bytes read so far followed by the rest of the stream)
    replay.write(prefix);
    pipeline(response.body, replay, () => {
      // Errors reach whoever reads `replay`
    });
    return new THTTPException(replayResponse(response, replay));
  }

  // The body is consumed now, so expose an equivalent response that can still be read
  replay.end(prefix);
  return new THTTPException(replayResponse(response, replay), getErrorDetailFromBody(prefix));
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
          return createHTTPException(response).then((error) => {
            throw error;
          });
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
