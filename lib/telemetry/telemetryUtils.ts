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

/**
 * Build full URL from host and path, always using HTTPS.
 * Strips any existing protocol prefix and enforces HTTPS.
 */
export default function buildTelemetryUrl(host: string, path: string): string {
  const cleanHost = host.replace(/^https?:\/\//, '').replace(/\/+$/, '');
  return `https://${cleanHost}${path}`;
}
