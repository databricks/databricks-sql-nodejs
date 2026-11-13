import { expect } from 'chai';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { v4 as uuid } from 'uuid';
import { DBSQLClient } from '../../../lib';
import StagingError from '../../../lib/errors/StagingError';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';
import { ErrorWithSqlState } from '../../../lib/kernel/KernelErrorMapping';
import config from '../utils/config';

describe('kernel volume file lifecycle', function volumeSuite() {
  this.timeout(120_000);
  [false, true].forEach((runAsync) => {
    it(`transfers bytes and reports failures with runAsync=${runAsync}`, async () => {
      const local = fs.mkdtempSync(path.join(os.tmpdir(), 'volume-'));
      const localPath = (value: string) => value.split(path.sep).join('/');
      const allowed = localPath(path.join(local, 'allowed'));
      fs.mkdirSync(allowed);
      const source = localPath(path.join(allowed, 'input.bin'));
      const destination = localPath(path.join(allowed, 'output.bin'));
      const denied = localPath(path.join(local, 'denied.bin'));
      const payload = Buffer.alloc(1024 * 1024);
      payload.forEach((_, i) => {
        payload[i] = i % 256;
      });
      fs.writeFileSync(source, payload);
      fs.writeFileSync(denied, payload);
      const remote = `/Volumes/${config.catalog}/${config.schema}/${config.volume}/${uuid()}.bin`;
      const client = new DBSQLClient();
      try {
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
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
          try {
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
              return failure as ErrorWithSqlState;
            };
            await expectTransferFailure(`PUT '${denied}' INTO '${remote}' OVERWRITE`);
            const missing = localPath(path.join(allowed, 'missing.bin'));
            let control: NodeJS.ErrnoException | undefined;
            try {
              fs.statSync(missing);
            } catch (error) {
              control = error as NodeJS.ErrnoException;
            }
            expect(control?.code).to.equal('ENOENT');
            expect(control?.errno).to.be.a('number');
            expect(control?.syscall).to.equal('stat');
            expect(control?.path).to.equal(missing);
            const failure = await expectTransferFailure(`PUT '${missing}' INTO '${remote}' OVERWRITE`);
            expect(failure.path).to.equal(missing);
            expect(failure.kernelMetadata?.osErrorFilename).to.equal(missing);
            expect(failure.kernelMetadata?.osErrorCode).to.be.a('number');
            expect(failure.kernelMetadata?.osErrorOperation).to.equal('stat');
            expect(failure.syscall).to.equal(control?.syscall);
            expect(failure.code).to.equal(control?.code);
            expect(failure.errno).to.equal(control?.errno);
            await execute(`REMOVE '${remote}'`);
            await execute(`REMOVE '${remote}'`);
            await expectTransferFailure(`GET '${remote}' TO '${localPath(path.join(allowed, 'deleted.bin'))}'`);
          } finally {
            await execute(`REMOVE '${remote}'`).catch(() => {});
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
