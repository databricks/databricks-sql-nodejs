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
 * Build full URL from host and path, handling protocol correctly.
 * @param host The hostname (with or without protocol)
 * @param path The path to append (should start with /)
 * @returns Full URL with protocol
 */
// eslint-disable-next-line import/prefer-default-export
export function buildUrl(host: string, path: string): string {
  // Check if host already has protocol
  if (host.startsWith('http://') || host.startsWith('https://')) {
    return `${host}${path}`;
  }
  // Add https:// if no protocol present
  return `https://${host}${path}`;
}
