import { ProgressUpdateResponse } from '../hive/Types';
import IOperation from '../contracts/IOperation';
import WaitUntilReady from './WaitUntilReady';
import IOperationResult from '../result/IOperationResult';
import GetResult from './GetResult';
import ProgressUpdateTransformer from './ProgressUpdateTransformer';

export default class HiveUtils {
  waitUntilReady(operation: IOperation, progress?: boolean, callback?: Function): Promise<IOperation> {
    const waitUntilReady = new WaitUntilReady(operation);

    return waitUntilReady.execute(progress, callback);
  }

  getResult(operation: IOperation, resultHandler?: IOperationResult): IOperationResult {
    const getResult = new GetResult(operation);

    return getResult.execute(resultHandler);
  }

  fetchAll(operation: IOperation): Promise<IOperation> {
    return operation.fetch().then(() => {
      if (operation.hasMoreRows()) {
        return this.fetchAll(operation);
      } else {
        return operation;
      }
    });
  }

  formatProgress(progressUpdate: ProgressUpdateResponse): string {
    return String(new ProgressUpdateTransformer(progressUpdate));
  }
}
