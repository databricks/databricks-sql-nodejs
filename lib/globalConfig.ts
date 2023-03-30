interface GlobalConfig {
  arrowEnabled?: boolean;
  useArrowNativeTypes?: boolean;

  retryMaxAttempts: number;
  retriesTimeout: number; // in milliseconds
  retryDelayMin: number; // in milliseconds
  retryDelayMax: number; // in milliseconds
}

export default {
  arrowEnabled: true,
  useArrowNativeTypes: true,

  retryMaxAttempts: 30,
  retriesTimeout: 900 * 1000,
  retryDelayMin: 1 * 1000,
  retryDelayMax: 60 * 1000,
} satisfies GlobalConfig;
