import TCLIService from '../../thrift/TCLIService';

type ThriftClient = TCLIService.Client;

type ThriftClientMethods = {
  [K in keyof ThriftClient]: ThriftClient[K];
};

export default interface IThriftClient extends ThriftClientMethods {}
