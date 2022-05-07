const TCLIService = require('../thrift/gen-nodejs/TCLIService');
const TCLIService_types = require('../thrift/gen-nodejs/TCLIService_types');
import _HiveClient from "./HiveClient";
import _HiveDriver from "./hive/HiveDriver";
import _HiveUtils from "./utils/HiveUtils";
import NoSaslAuthentication from "./connection/auth/NoSaslAuthentication";
import PlainHttpAuthentication from "./connection/auth/PlainHttpAuthentication";
import HttpConnection from "./connection/connections/HttpConnection";

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
