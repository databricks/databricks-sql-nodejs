# Troubleshooting

This section contains recipies for common errors.

## Lost connection

In this case, you have to reconnect `DBSQLClient`.

To determine if the connection is lost, you should subscribe to event `close`:

```javascript
client.on('close', () => {
    // do reconnect
});
```

Here is an example how you can manage a reconnection:

```javascript
const { DBSQLClient } = require('@databricks/sql');

const RECONNECT_ATTEMPTS = 50;
const RECONNECT_TIMEOUT = 3000; // millisecond

const client = new DBSQLClient();

client.on('close', () => {
    console.error('[Connection Lost]');

    connect(RECONNECT_ATTEMPTS).catch(error => {
        console.error('[Connection Failed]', error);
    });
});

connect(RECONNECT_ATTEMPTS).then(async client => {
    // work with client
}, (error) => {
    console.error('[Connection Failed]', error);
});

const connect = (attempts) => new Promise((resolve, reject) => {
    setTimeout(() => {
        client.connect(...).then((client) => {
            console.log('Connected successfully!');

            resolve(client);
        }, (error) => {
            console.error('[Connection Failed] attempt:' + attempts, error.message);

            if (!attempts) {
                reject(error);
            } else {
                connect(attempts - 1).then(resolve, reject);
            }
        });
    }, RECONNECT_TIMEOUT);
});
```

Please notice that you do not have to re-create client and pass it to your services.
After re-connection, the old client instance works just fine,
and you should be able to open session and work as you did before.
