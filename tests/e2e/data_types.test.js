const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const driver = require('../..');

const { TCLIService, TCLIService_types } = driver.thrift;

const utils = new driver.HiveUtils(
    TCLIService_types
);

const openSession = async () => {
    const client = new driver.DBSQLClient(
        TCLIService,
        TCLIService_types
    );

    const connection = await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
    });

    const session = await connection.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
    });

    if (config.database.length === 2) {
        await execute(session, `USE CATALOG ${config.database[0]}`);
        await execute(session, `USE DATABASE ${config.database[1]}`);
    }

    return session;
};

const execute = async (session, statement) => {
    const operation = await session.executeStatement(statement, { runAsync: true });

    await utils.waitUntilReady(operation, true, (stateResponse) => {
        logger(stateResponse.taskStatus);
    });
    await utils.fetchAll(operation);
    await operation.close();
    return utils.getResult(operation).getValue();
};

function removeTrailingMetadata(columns) {
    const result = [];
    for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        if (col.col_name === "") {
            break;
        }
        result.push(col);
    }
    return result;
}

describe('Data types', () => {
    it('primitive data types should presented correctly', async () => {
        const session = await openSession();
        try {
            await execute(session, `DROP TABLE IF EXISTS primitiveTypes`);
            await execute(session, `
                CREATE TABLE IF NOT EXISTS primitiveTypes (
                    bool
                    boolean,
                    tiny_int
                    tinyint,
                    small_int
                    smallint,
                    int_type
                    int,
                    big_int
                    bigint,
                    flt
                    float,
                    dbl
                    double,
                    dec
                    decimal(3, 2),
                    str string,
                    ts timestamp,
                    bin binary,
                    chr char(10),
                    vchr varchar(10),
                    dat date
                )
            `);

            const columns = await execute(session, 'describe primitiveTypes');
            expect(removeTrailingMetadata(columns)).to.be.deep.eq([
                {
                    "col_name": "bool",
                    "data_type": "boolean",
                    "comment": ""
                },
                {
                    "col_name": "tiny_int",
                    "data_type": "tinyint",
                    "comment": ""
                },
                {
                    "col_name": "small_int",
                    "data_type": "smallint",
                    "comment": ""
                },
                {
                    "col_name": "int_type",
                    "data_type": "int",
                    "comment": ""
                },
                {
                    "col_name": "big_int",
                    "data_type": "bigint",
                    "comment": ""
                },
                {
                    "col_name": "flt",
                    "data_type": "float",
                    "comment": ""
                },
                {
                    "col_name": "dbl",
                    "data_type": "double",
                    "comment": ""
                },
                {
                    "col_name": "dec",
                    "data_type": "decimal(3,2)",
                    "comment": ""
                },
                {
                    "col_name": "str",
                    "data_type": "string",
                    "comment": ""
                },
                {
                    "col_name": "ts",
                    "data_type": "timestamp",
                    "comment": ""
                },
                {
                    "col_name": "bin",
                    "data_type": "binary",
                    "comment": ""
                },
                {
                    "col_name": "chr",
                    "data_type": "char(10)",
                    "comment": ""
                },
                {
                    "col_name": "vchr",
                    "data_type": "varchar(10)",
                    "comment": ""
                },
                {
                    "col_name": "dat",
                    "data_type": "date",
                    "comment": ""
                }
            ]);

            await execute(session, `
                insert into primitiveTypes (
                    bool, tiny_int, small_int, int_type, big_int, flt, dbl,
                    dec, str, ts, bin, chr, vchr, dat
                ) values (
                    true, 127, 32000, 4000000, 372036854775807, 1.2, 2.2, 3.2, 'data',
                    '2014-01-17 00:17:13', 'data', 'a', 'b', '2014-01-17'
                )
            `);

            const records = await execute(session, 'select * from primitiveTypes');
            expect(records).to.be.deep.eq([
                {
                    "bool": true,
                    "tiny_int": 127,
                    "small_int": 32000,
                    "int_type": 4000000,
                    "big_int": 372036854775807,
                    "flt": 1.2,
                    "dbl": 2.2,
                    "dec": 3.2,
                    "str": "data",
                    "ts": "2014-01-17 00:17:13",
                    "bin": Buffer.from('data'),
                    "chr": "a",
                    "vchr": "b",
                    "dat": "2014-01-17"
                }
            ]);

            await session.close();
        } catch (error) {
            logger(error);
            await session.close()
            throw error;
        }
    });

    it('interval types should be presented correctly', async () => {
        const session = await openSession();
        try {
            await execute(session, `DROP TABLE IF EXISTS intervalTypes`);
            await execute(session, `
                CREATE TABLE intervalTypes AS
                SELECT INTERVAL '1' day AS day_interval, INTERVAL '1' month AS month_interval
            `);

            const columns = await execute(session, 'describe intervalTypes');
            expect(removeTrailingMetadata(columns)).to.be.deep.eq([
                {
                    "col_name": "day_interval",
                    "data_type": "interval day",
                    "comment": ""
                },
                {
                    "col_name": "month_interval",
                    "data_type": "interval month",
                    "comment": ""
                }
            ]);

            const records = await execute(session, 'select * from intervalTypes');
            expect(records).to.be.deep.eq([
                {
                    day_interval: "1 00:00:00.000000000",
                    month_interval: "0-1"
                }
            ]);

            await session.close();
        } catch (error) {
            logger(error);
            await session.close();
            throw error;
        }
    });

    it('complex types should be presented correctly', async () => {
        const session = await openSession();
        try {
            await execute(session, `DROP TABLE IF EXISTS dummy`);
            await execute(session, `DROP TABLE IF EXISTS complexTypes`);
            await execute(session, `create table dummy( id string )`);
            await execute(session, `insert into dummy (id) values (1)`);
            await execute(session, `
                CREATE TABLE complexTypes (
                    id int,
                    arr_type array<string>,
                    map_type map<string, int>,
                    struct_type struct<city:string,state:string>
                )
            `);

            const columns = await execute(session, 'describe complexTypes');
            expect(removeTrailingMetadata(columns)).to.be.deep.eq([
                {
                    "col_name": "id",
                    "data_type": "int",
                    "comment": ""
                },
                {
                    "col_name": "arr_type",
                    "data_type": "array<string>",
                    "comment": ""
                },
                {
                    "col_name": "map_type",
                    "data_type": "map<string,int>",
                    "comment": ""
                },
                {
                    "col_name": "struct_type",
                    "data_type": "struct<city:string,state:string>",
                    "comment": ""
                }
            ]);

            await execute(session, `
                INSERT INTO table complexTypes SELECT
                    POSITIVE(1) as id,
                    array('a', 'b') as arr_type,
                    map('key', 12) as map_type,
                    named_struct('city','Tampa','State','FL') as struct_type
                FROM dummy
            `);
            await execute(session, `
                INSERT INTO table complexTypes SELECT
                    POSITIVE(2) as id,
                    array('c', 'd') as arr_type,
                    map('key2', 12) as map_type,
                    named_struct('city','Albany','State','NY') as struct_type
                FROM dummy
            `);
            await execute(session, `
                INSERT INTO table complexTypes SELECT
                    POSITIVE(3) as id,
                    array('e', 'd') as arr_type,
                    map('key2', 13) as map_type,
                    named_struct('city','Los Angeles','State','CA') as struct_type
                FROM dummy
            `);

            const records = await execute(session, 'select * from complexTypes order by id asc');
            expect(records).to.be.deep.eq([
                {
                    "id": 1,
                    "arr_type": [
                        "a",
                        "b"
                    ],
                    "map_type": {
                        "key": 12
                    },
                    "struct_type": {
                        "city": "Tampa",
                        "state": "FL"
                    }
                },
                {
                    "id": 2,
                    "arr_type": [
                        "c",
                        "d"
                    ],
                    "map_type": {
                        "key2": 12
                    },
                    "struct_type": {
                        "city": "Albany",
                        "state": "NY"
                    }
                },
                {
                    "id": 3,
                    "arr_type": [
                        "e",
                        "d"
                    ],
                    "map_type": {
                        "key2": 13
                    },
                    "struct_type": {
                        "city": "Los Angeles",
                        "state": "CA"
                    }
                }
            ]);

            await session.close();
        } catch(error) {
            logger(error);
            await session.close()
            throw error;
        }
    });
});
