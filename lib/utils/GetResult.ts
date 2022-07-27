import IOperationResult from '../result/IOperationResult';
import NullResult from '../result/NullResult';
import JsonResult from '../result/JsonResult';
import IOperation from '../contracts/IOperation';

export default class GetResult {
  private operation: IOperation;

  constructor(operation: IOperation) {
    this.operation = operation;
  }

  /**
   * Combines operation schema and data
   *
   * @param resultHandler you may specify your own result combiner to implement IOperationResult and pass as paramenter.
   *                      If resultHandler is not specified, the internal handler will interpret result as Json.
   */
  execute(resultHandler?: IOperationResult): IOperationResult {
    if (!resultHandler) {
      resultHandler = this.getDefaultHandler();
    }

    resultHandler.setOperation(this.operation);

    return resultHandler;
  }

  private getDefaultHandler(): IOperationResult {
    const schema = this.operation.getSchema();

    if (schema === null) {
      return new NullResult();
    } else {
      return new JsonResult();
    }
  }
}
