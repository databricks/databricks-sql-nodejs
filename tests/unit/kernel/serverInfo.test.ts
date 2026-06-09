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

import { expect } from 'chai';
import {
  kernelServerInfoValue,
  KERNEL_DBMS_NAME,
  KERNEL_SERVER_NAME,
  KERNEL_DBMS_VERSION,
} from '../../../lib/kernel/KernelServerInfo';
import { TGetInfoType } from '../../../thrift/TCLIService_types';

describe('KernelServerInfo.kernelServerInfoValue', () => {
  it('CLI_DBMS_NAME matches Thrift exactly ("Spark SQL")', () => {
    expect(kernelServerInfoValue(TGetInfoType.CLI_DBMS_NAME)?.stringValue).to.equal('Spark SQL');
    expect(KERNEL_DBMS_NAME).to.equal('Spark SQL');
  });

  it('CLI_DBMS_VER matches the Thrift server version constant', () => {
    expect(kernelServerInfoValue(TGetInfoType.CLI_DBMS_VER)?.stringValue).to.equal('3.1.1');
    expect(KERNEL_DBMS_VERSION).to.equal('3.1.1');
  });

  it('CLI_SERVER_NAME matches Thrift exactly ("Spark SQL")', () => {
    const v = kernelServerInfoValue(TGetInfoType.CLI_SERVER_NAME)?.stringValue;
    expect(v).to.equal(KERNEL_SERVER_NAME);
    expect(v).to.equal('Spark SQL');
  });

  it('all three answered info types are byte-identical to Thrift', () => {
    expect(kernelServerInfoValue(TGetInfoType.CLI_SERVER_NAME)?.stringValue).to.equal('Spark SQL');
    expect(kernelServerInfoValue(TGetInfoType.CLI_DBMS_NAME)?.stringValue).to.equal('Spark SQL');
    expect(kernelServerInfoValue(TGetInfoType.CLI_DBMS_VER)?.stringValue).to.equal('3.1.1');
  });

  it('returns undefined for info types the Thrift server rejects (e.g. CLI_MAX_DRIVER_CONNECTIONS)', () => {
    expect(kernelServerInfoValue(TGetInfoType.CLI_MAX_DRIVER_CONNECTIONS)).to.equal(undefined);
    expect(kernelServerInfoValue(TGetInfoType.CLI_USER_NAME)).to.equal(undefined);
    expect(kernelServerInfoValue(99999)).to.equal(undefined);
  });
});
