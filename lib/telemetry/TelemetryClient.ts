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

import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';

/**
 * Telemetry client for a specific host.
 * Managed by TelemetryClientProvider with reference counting.
 * One client instance is shared across all connections to the same host.
 */
class TelemetryClient {
  private closed: boolean = false;

  constructor(private context: IClientContext, private host: string) {
    // Client created silently
  }

  /**
   * Gets the host associated with this client.
   */
  getHost(): string {
    return this.host;
  }

  /**
   * Checks if the client has been closed.
   */
  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Closes the telemetry client and releases resources.
   * Should only be called by TelemetryClientProvider when reference count reaches zero.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }

    try {
      this.closed = true;
    } catch (error: any) {
      // Swallow all exceptions per requirement
      this.closed = true;
      try {
        const logger = this.context.getLogger();
        logger.log(LogLevel.debug, `Telemetry close error: ${error.message}`);
      } catch (logError: any) {
        // If even logging fails, silently swallow
      }
    }
  }
}

export default TelemetryClient;
