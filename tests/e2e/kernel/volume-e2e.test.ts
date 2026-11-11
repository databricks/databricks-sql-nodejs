import { expect } from 'chai';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { v4 as uuid } from 'uuid';
import { DBSQLClient } from '../../../lib';
import StagingError from '../../../lib/errors/StagingError';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';

describe('kernel volume file lifecycle', function volumeSuite() {
  this.timeout(120_000);
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const httpPath = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL;

  before(function gate() {
    if (!host || !httpPath || !token) this.skip();
  });

  [false, true].forEach((runAsync) => {
    it(`transfers bytes and reports failures with runAsync=${runAsync}`, async () => {
      const local = fs.mkdtempSync(path.join(os.tmpdir(), 'volume-'));
      const allowed = path.join(local, 'allowed');
      fs.mkdirSync(allowed);
      const source = path.join(allowed, 'input.bin');
      const destination = path.join(allowed, 'output.bin');
      const denied = path.join(local, 'denied.bin');
      const payload = Buffer.alloc(1024 * 1024);
      payload.forEach((_, i) => {
        payload[i] = i % 256;
      });
      fs.writeFileSync(source, payload);
      fs.writeFileSync(denied, payload);
      const namespace = `volume_${uuid().replace(/-/g, '')}`;
      const remote = `/Volumes/main/${namespace}/files/input.bin`;
      const client = new DBSQLClient();
      try {
        await client.connect({
          host: host!,
          path: httpPath!,
          token: token!,
          useKernel: true,
        } as ConnectionOptions & InternalConnectionOptions);
        const session = await client.openSession();
        try {
          const execute = async (query: string) => {
            const operation = await session.executeStatement(query, { runAsync, stagingAllowedLocalPath: [allowed] });
            try {
              await operation.finished();
              return await operation.fetchAll();
            } finally {
              await operation.close();
            }
          };
          await execute(`CREATE SCHEMA main.${namespace}`);
          try {
            await execute(`CREATE VOLUME main.${namespace}.files`);
            expect(await execute(`PUT '${source}' INTO '${remote}' OVERWRITE`)).to.deep.equal([]);
            expect(await execute(`GET '${remote}' TO '${destination}'`)).to.deep.equal([]);
            expect(fs.readFileSync(destination).equals(payload)).to.equal(true);
            const expectTransferFailure = async (query: string) => {
              let failure: unknown;
              try {
                await execute(query);
              } catch (error) {
                failure = error;
              }
              expect(failure).to.be.instanceOf(StagingError);
            };
            await expectTransferFailure(`PUT '${denied}' INTO '${remote}' OVERWRITE`);
            await expectTransferFailure(`PUT '${path.join(allowed, 'missing.bin')}' INTO '${remote}' OVERWRITE`);
            await execute(`REMOVE '${remote}'`);
            await execute(`REMOVE '${remote}'`);
            let deleted: unknown;
            try {
              await execute(`GET '${remote}' TO '${path.join(allowed, 'deleted.bin')}'`);
            } catch (error) {
              deleted = error;
            }
            expect(deleted).to.be.instanceOf(StagingError);
          } finally {
            await execute(`DROP SCHEMA main.${namespace} CASCADE`);
          }
        } finally {
          await session.close();
        }
      } finally {
        await client.close();
        fs.rmSync(local, { recursive: true, force: true });
      }
    });
  });
});
