// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { TGetInfoType, TGetInfoValue } from '../../thrift/TCLIService_types';

/** ODBC `SQL_TXN_SERIALIZABLE`. */
export const SQL_TXN_SERIALIZABLE = 8;

/**
 * Info types answered by the driver itself on every backend, because the server
 * rejects them. Delta transactions are serializable-class, so (like the
 * Databricks ODBC driver) we advertise `SQL_TXN_SERIALIZABLE` as both the
 * default and the only supported isolation level. Returns `undefined` for every
 * other info type.
 */
export function driverInfoValue(infoType: number): TGetInfoValue | undefined {
  switch (infoType) {
    case TGetInfoType.CLI_DEFAULT_TXN_ISOLATION:
      return new TGetInfoValue({ integerFlag: SQL_TXN_SERIALIZABLE });
    case TGetInfoType.CLI_TXN_ISOLATION_OPTION:
      return new TGetInfoValue({ integerBitmask: SQL_TXN_SERIALIZABLE });
    default:
      return undefined;
  }
}
