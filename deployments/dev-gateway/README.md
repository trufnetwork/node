# Testing the Gateway Locally
These files are not required to run TN locally, however they are useful for testing the gateway locally.

## Steps
- Make sure you have downloaded the privately shared Kwil Gateway. If you have not, and need to test it, please contact the TN team.
- Move the expected binary (linux amd) to `.build/kgw`
- Start the TN compose file, to ensure the necessary network is created
- Start the gateway compose file with `docker-compose -f dev-gateway-compose.yaml up`
- To trust the localhost certificate, run `task setup:local-cert`
- To generate a new certificate, run `task setup:new-local-cert`

## Testing
You may run `kwil-cli utils ping --provider https://localhost:443` and should receive `pong` as a response.
