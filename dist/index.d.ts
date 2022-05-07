import _HiveClient from "./HiveClient";
import _HiveDriver from "./hive/HiveDriver";
import _HiveUtils from "./utils/HiveUtils";
import NoSaslAuthentication from "./connection/auth/NoSaslAuthentication";
import PlainHttpAuthentication from "./connection/auth/PlainHttpAuthentication";
import HttpConnection from "./connection/connections/HttpConnection";
export declare const auth: {
    NoSaslAuthentication: typeof NoSaslAuthentication;
    PlainHttpAuthentication: typeof PlainHttpAuthentication;
};
export declare const connections: {
    HttpConnection: typeof HttpConnection;
};
export declare const thrift: {
    TCLIService: any;
    TCLIService_types: any;
};
export declare class HiveClient extends _HiveClient {
}
export declare class HiveDriver extends _HiveDriver {
}
export declare class HiveUtils extends _HiveUtils {
}
