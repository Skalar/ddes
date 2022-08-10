# DDES kitchen sink example

## Example usage

```shell
# Run stores (postgres, dynamodb) via docker
docker compose up -d

# Prepare stores
yarn ts-node-script support/setup.ts

# Choose a store
export STORE=postgres

# Run account stats (in a separate shell)
yarn ts-node-script consumer-examples/account-stats

# Simulate projection (in a separate shell)
yarn ts-node-script consumer-examples/projector

# Generate some events
yarn ts-node-script commands/open-account mycompany taxes
yarn ts-node-script commands/deposit-money mycompany taxes 500
yarn ts-node-script commands/withdraw-money mycompany taxes 1
yarn ts-node-script commands/open-account mycompany payroll

# Stream commits (version >= 3) from a single account (in a separate shell)
yarn ts-node-script consumer-examples/stream-account-commits mycompany taxes 3

# Add some money to the account we are streaming commits from
yarn ts-node-script commands/deposit-money mycompany taxes 100
 
# Shut down and remove stores
docker compose down
```
