import stagingTests from '../utils/stagingTests';

const kernelDescribe =
  process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME &&
  process.env.DATABRICKS_PECOTESTING_HTTP_PATH &&
  process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL
    ? describe
    : describe.skip;

kernelDescribe('Kernel Staging Test', () => {
  stagingTests(() => ({
    host: process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME as string,
    path: process.env.DATABRICKS_PECOTESTING_HTTP_PATH as string,
    token: process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL as string,
    useKernel: true,
  }));
});
