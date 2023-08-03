import { TRowSet } from '../../thrift/TCLIService_types';

export default interface IOperationResult {
  getValue(data?: Array<TRowSet>): Promise<any>;
}
