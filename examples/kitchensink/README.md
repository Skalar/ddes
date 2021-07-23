# DDES kitchen sink example

## Example usage

```shell
# Install node dependencies
yarn

# Run stores (postgres, dynamodb) via docker
docker compose up -d

# Prepare stores
support/setup.ts

# Run account stats (in separate shell)
STORE=dynamodb consumer-examples/account-stats.ts

# Generate some events
STORE=dynamodb commands/open-account.ts mycompany taxes
STORE=dynamodb commands/deposit-money.ts mycompany taxes 500
STORE=dynamodb commands/withdraw-money.ts mycompany taxes 1
STORE=dynamodb commands/open-account.ts mycompany payroll

# Shut down and remove stores
docker compose down
```
