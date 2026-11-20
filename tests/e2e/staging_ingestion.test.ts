import stagingTests from './utils/stagingTests';
import config from './utils/config';

describe('Staging Test', () => {
  stagingTests(() => ({
    host: config.host,
    path: config.path,
    token: config.token,
  }));
});
