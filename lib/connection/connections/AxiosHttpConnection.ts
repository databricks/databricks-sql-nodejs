/*
  This file is created using node_modules/thrift/lib/nodejs/lib/thrift/http_connection.js as an example
*/

import { EventEmitter } from 'events';
import { TBinaryProtocol, TBufferedTransport, Thrift, TProtocol, TProtocolConstructor, TTransport } from 'thrift';
import axios, { AxiosRequestConfig, AxiosResponse } from 'axios';
// @ts-expect-error TS7016: Could not find a declaration file for module
import InputBufferUnderrunError from 'thrift/lib/nodejs/lib/thrift/input_buffer_underrun_error';

export class THTTPException extends Thrift.TApplicationException {
  public readonly statusCode: unknown;

  public readonly response: AxiosResponse;

  constructor(response: AxiosResponse) {
    super(
      Thrift.TApplicationExceptionType.PROTOCOL_ERROR,
      `Received a response with a bad HTTP status code: ${response.status}`,
    );
    this.statusCode = response.status;
    this.response = response;
  }
}

type TTransportType = typeof TBufferedTransport;

interface ThriftHttpConnectionOptions {
  transport?: TTransportType;
  protocol?: TProtocolConstructor;
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

// TODO: Cookie handling - ?
export default class AxiosHttpConnection extends EventEmitter {
  private readonly config: AxiosRequestConfig;

  // This field is used by Thrift internally, so name and type are important
  private readonly transport: TTransportType;

  // This field is used by Thrift internally, so name and type are important
  private readonly protocol: TProtocolConstructor;

  // thrift.createClient sets this field internally
  public client?: ThriftClient;

  constructor(config: AxiosRequestConfig, options: ThriftHttpConnectionOptions = {}) {
    super();
    this.config = config;
    this.transport = options.transport ?? TBufferedTransport;
    this.protocol = options.protocol ?? TBinaryProtocol;
  }

  public write(data: Buffer, seqId: number) {
    const axiosConfig: AxiosRequestConfig = {
      ...this.config,
      method: 'POST',
      headers: {
        ...this.config.headers,
        Connection: 'keep-alive',
        'Content-Length': data.length,
        'Content-Type': 'application/x-thrift',
      },
      data,
      responseType: 'arraybuffer',
      timeoutErrorMessage: 'Request timed out',
    };

    axios
      .request(axiosConfig)
      .then((response) => {
        if (response.status !== 200) {
          throw new THTTPException(response);
        }

        const buffer = Buffer.from(response.data);
        this.transport.receiver((transportWithData) => this.handleThriftResponse(transportWithData), seqId)(buffer);
      })
      .catch((error) => {
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
