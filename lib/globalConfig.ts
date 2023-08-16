interface GlobalConfig {
  arrowEnabled?: boolean;
  useArrowNativeTypes?: boolean;
  socketTimeout: number;

  retryMaxAttempts: number;
  retriesTimeout: number; // in milliseconds
  retryDelayMin: number; // in milliseconds
  retryDelayMax: number; // in milliseconds

  useCloudFetch: boolean;
  cloudFetchConcurrentDownloads: number;
}

export default {
  arrowEnabled: true,
  useArrowNativeTypes: true,
  socketTimeout: 15 * 60 * 1000, // 15 minutes

  retryMaxAttempts: 30,
  retriesTimeout: 900 * 1000,
  retryDelayMin: 1 * 1000,
  retryDelayMax: 60 * 1000,

  useCloudFetch: false,
  cloudFetchConcurrentDownloads: 10,
} satisfies GlobalConfig;
