/// <reference types="node" />
import HiveUtils from "./utils/HiveUtils";
import IHiveSession from "./contracts/IHiveSession";
interface EventEmitter extends NodeJS.EventEmitter {
}
interface IConnectionOptions {
    host: string;
    port?: number;
    path: string;
    token: string;
    clientId?: string;
}
/**
 * @see IHiveClient
 */
interface IDBSQLClient {
    connect(options: IConnectionOptions): Promise<IDBSQLClient>;
    openSession(): Promise<IHiveSession>;
    close(): void;
}
export default class DBSQLClient implements IDBSQLClient, EventEmitter {
    static utils: HiveUtils;
    private client;
    private getUserAgent;
    connect(options: IConnectionOptions): Promise<this>;
    openSession(): Promise<IHiveSession>;
    close(): void;
    addListener(event: string | symbol, listener: (...args: any[]) => void): this;
    on(event: string | symbol, listener: (...args: any[]) => void): this;
    once(event: string | symbol, listener: (...args: any[]) => void): this;
    removeListener(event: string | symbol, listener: (...args: any[]) => void): this;
    off(event: string | symbol, listener: (...args: any[]) => void): this;
    removeAllListeners(event?: string | symbol): this;
    setMaxListeners(n: number): this;
    getMaxListeners(): number;
    listeners(event: string | symbol): Function[];
    rawListeners(event: string | symbol): Function[];
    emit(event: string | symbol, ...args: any[]): boolean;
    listenerCount(type: string | symbol): number;
    prependListener(event: string | symbol, listener: (...args: any[]) => void): this;
    prependOnceListener(event: string | symbol, listener: (...args: any[]) => void): this;
    eventNames(): (string | symbol)[];
}
export {};
