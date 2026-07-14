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
import DBSQLLogger from '../../lib/DBSQLLogger';
import { LogLevel } from '../../lib/contracts/IDBSQLLogger';

describe('DBSQLLogger level subscription', () => {
  it('getLevel reflects the configured level', () => {
    const logger = new DBSQLLogger({ level: LogLevel.warn });
    expect(logger.getLevel()).to.equal(LogLevel.warn);
  });

  it('onLevelChange fires subscribers with the new level on setLevel', () => {
    const logger = new DBSQLLogger({ level: LogLevel.info });
    const seen: LogLevel[] = [];
    logger.onLevelChange((level) => seen.push(level));

    logger.setLevel(LogLevel.debug);
    logger.setLevel(LogLevel.error);

    expect(seen).to.deep.equal([LogLevel.debug, LogLevel.error]);
    // setLevel still updates the logger's own level.
    expect(logger.getLevel()).to.equal(LogLevel.error);
  });

  it('the returned unsubscribe stops further notifications', () => {
    const logger = new DBSQLLogger({ level: LogLevel.info });
    const seen: LogLevel[] = [];
    const unsubscribe = logger.onLevelChange((level) => seen.push(level));

    logger.setLevel(LogLevel.debug);
    unsubscribe();
    logger.setLevel(LogLevel.warn);

    expect(seen).to.deep.equal([LogLevel.debug]);
  });

  it('a throwing subscriber does not break setLevel or other subscribers', () => {
    const logger = new DBSQLLogger({ level: LogLevel.info });
    const seen: LogLevel[] = [];
    logger.onLevelChange(() => {
      throw new Error('boom');
    });
    logger.onLevelChange((level) => seen.push(level));

    expect(() => logger.setLevel(LogLevel.debug)).to.not.throw();
    expect(seen).to.deep.equal([LogLevel.debug]);
    expect(logger.getLevel()).to.equal(LogLevel.debug);
  });
});
