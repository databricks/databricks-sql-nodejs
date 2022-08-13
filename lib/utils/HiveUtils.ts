import { TProgressUpdateResp } from '../../thrift/TCLIService_types';
import IOperation from '../contracts/IOperation';
import ProgressUpdateTransformer from './ProgressUpdateTransformer';

export default class HiveUtils {
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
