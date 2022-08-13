import { TProgressUpdateResp } from '../../thrift/TCLIService_types';
import IOperation from '../contracts/IOperation';
import WaitUntilReady from './WaitUntilReady';
import ProgressUpdateTransformer from './ProgressUpdateTransformer';

export default class HiveUtils {
  waitUntilReady(operation: IOperation, progress?: boolean, callback?: Function): Promise<IOperation> {
    const waitUntilReady = new WaitUntilReady(operation);

    return waitUntilReady.execute(progress, callback);
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

  formatProgress(progressUpdate: TProgressUpdateResp): string {
    return String(new ProgressUpdateTransformer(progressUpdate));
  }
}
