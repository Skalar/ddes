# DDES kitchen sink example

## Example usage

```shell
# Install node dependencies
yarn

# Run stores (postgres, dynamodb) via docker
docker compose up -d

# Prepare stores
support/setup.ts

# Choose a store
export STORE=postgres

# Run account stats (in a separate shell)
consumer-examples/account-stats.ts

# Simulate projection (in a separate shell)
consumer-examples/projector.ts

# Generate some events
commands/open-account.ts mycompany taxes
commands/deposit-money.ts mycompany taxes 500
commands/withdraw-money.ts mycompany taxes 1
commands/open-account.ts mycompany payroll

# Stream commits (version >= 3) from a single account (in a separate shell)
consumer-examples/stream-account-commits.ts mycompany taxes 3

# Add some money to the account we are streaming commits from
commands/deposit-money.ts mycompany taxes 100
 
# Shut down and remove stores
docker compose down
```
