import fetch from 'node-fetch';

import {
  ExecuteStatementRequest,
  ExecuteStatementResponse,
  CancelExecutionRequest,
  GetStatementRequest,
  GetStatementResponse,
  GetStatementResultChunkNRequest,
  ResultData,
} from './Types';

export interface RestClientOptions {
  host: string;
  warehouseId: string;
  headers?: Record<string, string>;
}

enum HTTPMethod {
  GET = 'GET',
  POST = 'POST',
}

export default class RestClient {
  private readonly options: RestClientOptions;

  private async doRequest<P, R>(method: string, path: string, payload: P): Promise<R> {
    const { host, headers } = this.options;
    const response = await fetch(`https://${host}${path}`, {
      method,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        ...headers,
      },
      body: method !== HTTPMethod.GET ? JSON.stringify(payload) : undefined,
    });
    const result = await response.json();
    return result as R;
  }

  constructor(options: RestClientOptions) {
    this.options = options;
  }

  public getWarehouseId() {
    return this.options.warehouseId;
  }

  public executeStatement(request: ExecuteStatementRequest) {
    return this.doRequest<ExecuteStatementRequest, ExecuteStatementResponse>(
      HTTPMethod.POST,
      '/api/2.0/sql/statements/',
      request,
    );
  }

  public cancelExecution(request: CancelExecutionRequest) {
    return this.doRequest<CancelExecutionRequest, undefined>(
      HTTPMethod.POST,
      `/api/2.0/sql/statements/${request.statement_id}/cancel`,
      request,
    );
  }

  public getStatement(request: GetStatementRequest): Promise<GetStatementResponse> {
    return this.doRequest<GetStatementRequest, GetStatementResponse>(
      HTTPMethod.GET,
      `/api/2.0/sql/statements/${request.statement_id}`,
      request,
    );
  }

  public getStatementResultChunkN(request: GetStatementResultChunkNRequest): Promise<ResultData> {
    return this.doRequest<GetStatementResultChunkNRequest, ResultData>(
      HTTPMethod.GET,
      `/api/2.0/sql/statements/${request.statement_id}/result/chunks/${request.chunk_index}`,
      request,
    );
  }

  public getStatementResultChunk(internalLink: string): Promise<ResultData> {
    return this.doRequest<void, ResultData>(
      HTTPMethod.GET,
      internalLink,
      undefined,
    );
  }

  public async fetchExternalLink(url: string): Promise<Buffer> {
    const { host, headers } = this.options;
    const response = await fetch(url, {
      method: HTTPMethod.GET,
      headers: {},
    });
    const result = await response.arrayBuffer();
    return Buffer.from(result);
  }
}
