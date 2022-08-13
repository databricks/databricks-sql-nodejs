import IOperationResult from './IOperationResult';

export default class NullResult implements IOperationResult {
  getValue(): null {
    return null;
  }
}
