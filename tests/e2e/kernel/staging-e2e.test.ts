import stagingTests from '../utils/stagingTests';
import config from '../utils/config';

const kernelDescribe =
  process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME &&
  process.env.DATABRICKS_PECOTESTING_HTTP_PATH &&
  process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL
    ? describe
    : describe.skip;

kernelDescribe('Kernel Staging Test', () => {
  stagingTests(() => ({
    host: config.host,
    path: config.path,
    token: config.token,
    useKernel: true,
  }));
});
