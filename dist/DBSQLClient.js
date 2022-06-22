"use strict";
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var TCLIService = require('../thrift/gen-nodejs/TCLIService');
var TCLIService_types = require('../thrift/gen-nodejs/TCLIService_types');
var HiveClient_1 = __importDefault(require("./HiveClient"));
var HiveUtils_1 = __importDefault(require("./utils/HiveUtils"));
var PlainHttpAuthentication_1 = __importDefault(require("./connection/auth/PlainHttpAuthentication"));
var HttpConnection_1 = __importDefault(require("./connection/connections/HttpConnection"));
var DBSQLClient = /** @class */ (function () {
    function DBSQLClient() {
        this.client = new HiveClient_1.default(TCLIService, TCLIService_types);
    }
    DBSQLClient.prototype.connect = function (options) {
        var _this = this;
        return this.client.connect({
            host: options.host,
            port: options.port || 443,
            options: {
                path: options.path,
                https: true,
            }
        }, new HttpConnection_1.default(), new PlainHttpAuthentication_1.default({
            username: 'token',
            password: options.token,
        })).then(function () { return _this; });
    };
    DBSQLClient.prototype.openSession = function () {
        return this.client.openSession({
            client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11
        });
    };
    DBSQLClient.prototype.close = function () {
        this.client.close();
    };
    // EventEmitter
    DBSQLClient.prototype.addListener = function (event, listener) {
        this.client.addListener(event, listener);
        return this;
    };
    DBSQLClient.prototype.on = function (event, listener) {
        this.client.on(event, listener);
        return this;
    };
    DBSQLClient.prototype.once = function (event, listener) {
        this.client.once(event, listener);
        return this;
    };
    DBSQLClient.prototype.removeListener = function (event, listener) {
        this.client.removeListener(event, listener);
        return this;
    };
    DBSQLClient.prototype.off = function (event, listener) {
        this.client.off(event, listener);
        return this;
    };
    DBSQLClient.prototype.removeAllListeners = function (event) {
        this.client.removeAllListeners(event);
        return this;
    };
    DBSQLClient.prototype.setMaxListeners = function (n) {
        this.client.setMaxListeners(n);
        return this;
    };
    DBSQLClient.prototype.getMaxListeners = function () {
        return this.client.getMaxListeners();
    };
    DBSQLClient.prototype.listeners = function (event) {
        return this.client.listeners(event);
    };
    DBSQLClient.prototype.rawListeners = function (event) {
        return this.client.rawListeners(event);
    };
    DBSQLClient.prototype.emit = function (event) {
        var _a;
        var args = [];
        for (var _i = 1; _i < arguments.length; _i++) {
            args[_i - 1] = arguments[_i];
        }
        return (_a = this.client).emit.apply(_a, __spreadArray([event], args, false));
    };
    DBSQLClient.prototype.listenerCount = function (type) {
        return this.client.listenerCount(type);
    };
    DBSQLClient.prototype.prependListener = function (event, listener) {
        this.client.prependListener(event, listener);
        return this;
    };
    DBSQLClient.prototype.prependOnceListener = function (event, listener) {
        this.client.prependOnceListener(event, listener);
        return this;
    };
    DBSQLClient.prototype.eventNames = function () {
        return this.client.eventNames();
    };
    DBSQLClient.utils = new HiveUtils_1.default(TCLIService_types);
    return DBSQLClient;
}());
exports.default = DBSQLClient;
//# sourceMappingURL=DBSQLClient.js.map