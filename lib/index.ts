const TCLIService = require('../thrift/gen-nodejs/TCLIService');
const TCLIService_types = require('../thrift/gen-nodejs/TCLIService_types');
import _HiveClient from "./HiveClient";
import _HiveDriver from "./hive/HiveDriver";
import _HiveUtils from "./utils/HiveUtils";
import NoSaslAuthentication from "./connection/auth/NoSaslAuthentication";
import PlainHttpAuthentication from "./connection/auth/PlainHttpAuthentication";
import HttpConnection from "./connection/connections/HttpConnection";

import { OpenSessionRequest } from "./hive/Commands/OpenSessionCommand";
import IHiveClient from './contracts/IHiveClient';
import IHiveSession from "./contracts/IHiveSession";

export const auth = {
    NoSaslAuthentication,
    PlainHttpAuthentication,
};

export const connections = {
    HttpConnection,
};

export const thrift = {
    TCLIService,
    TCLIService_types
};

export class HiveClient extends _HiveClient {}
export class HiveDriver extends _HiveDriver {}
export class HiveUtils extends _HiveUtils  {}

interface IConnectionOptions {
    host: string;
    port?: number;
    path: string;
    token: string,
}

export class DBSQLClient extends HiveClient {
    // @ts-expect-error Need to replace inheritance with composition and just redirect other methods
    connect(options: IConnectionOptions): Promise<IHiveClient> {
        return super.connect(
          {
              host: options.host,
              port: options.port || 443,
              options: {
                  path: options.path,
                  https: true,
              }
          },
          new HttpConnection(),
          new PlainHttpAuthentication({
              username: 'token',
              password: options.token,
          }),
        );
    }

    openSession(): Promise<IHiveSession> {
        return super.openSession({
          client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11
        });
    }
}
