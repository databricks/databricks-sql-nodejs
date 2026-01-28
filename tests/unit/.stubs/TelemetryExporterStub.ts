/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TelemetryMetric } from '../../../lib/telemetry/types';

/**
 * Stub implementation of DatabricksTelemetryExporter for testing.
 * Records exported metrics for verification in tests.
 */
export default class TelemetryExporterStub {
  public exportedMetrics: TelemetryMetric[][] = [];
  public exportCount = 0;
  public shouldThrow = false;
  public throwError: Error | null = null;

  /**
   * Stub export method that records metrics.
   */
  async export(metrics: TelemetryMetric[]): Promise<void> {
    this.exportCount++;
    this.exportedMetrics.push([...metrics]);

    if (this.shouldThrow && this.throwError) {
      throw this.throwError;
    }
  }

  /**
   * Reset the stub state.
   */
  reset(): void {
    this.exportedMetrics = [];
    this.exportCount = 0;
    this.shouldThrow = false;
    this.throwError = null;
  }

  /**
   * Get all exported metrics flattened.
   */
  getAllExportedMetrics(): TelemetryMetric[] {
    return this.exportedMetrics.flat();
  }

  /**
   * Configure stub to throw an error on export.
   */
  throwOnExport(error: Error): void {
    this.shouldThrow = true;
    this.throwError = error;
  }
}
