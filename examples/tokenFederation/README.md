# Token Federation Examples

Examples demonstrating the token provider and federation features of the Databricks SQL Node.js Driver.

## Examples

### Static Token (`staticToken.ts`)

The simplest authentication method. Use a static access token that doesn't change during the application lifetime.

```bash
DATABRICKS_HOST=<host> DATABRICKS_HTTP_PATH=<path> DATABRICKS_TOKEN=<token> npx ts-node staticToken.ts
```

### External Token (`externalToken.ts`)

Use a callback function to provide tokens dynamically. Useful for integrating with secret managers, vaults, or other token sources. Tokens are automatically cached by the driver.

```bash
DATABRICKS_HOST=<host> DATABRICKS_HTTP_PATH=<path> DATABRICKS_TOKEN=<token> npx ts-node externalToken.ts
```

### Token Federation (`federation.ts`)

Automatically exchange tokens from external identity providers (Azure AD, Google, Okta, etc.) for Databricks-compatible tokens using RFC 8693 token exchange.

```bash
DATABRICKS_HOST=<host> DATABRICKS_HTTP_PATH=<path> AZURE_AD_TOKEN=<token> npx ts-node federation.ts
```

### M2M Federation (`m2mFederation.ts`)

Machine-to-machine token federation with a service principal. Requires a `federationClientId` to identify the service principal to Databricks.

```bash
DATABRICKS_HOST=<host> DATABRICKS_HTTP_PATH=<path> DATABRICKS_CLIENT_ID=<client-id> SERVICE_ACCOUNT_TOKEN=<token> npx ts-node m2mFederation.ts
```

### Custom Token Provider (`customTokenProvider.ts`)

Implement the `ITokenProvider` interface for full control over token management, including custom caching, refresh logic, retry, and error handling.

```bash
DATABRICKS_HOST=<host> DATABRICKS_HTTP_PATH=<path> OAUTH_SERVER_URL=<url> OAUTH_CLIENT_ID=<id> OAUTH_CLIENT_SECRET=<secret> npx ts-node customTokenProvider.ts
```

## Prerequisites

- Node.js 14+
- A Databricks workspace with token federation enabled (for federation examples)
- Valid credentials for your identity provider
