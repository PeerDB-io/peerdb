# Instructions for running Flow e2e tests
1. Run the catalog docker instance and point it to port 7132.
  See <repo>/stacks/catalog.docker-compose.yml and edit the port mapping to 7132:5432.
2. Run the catalog using docker compose --file stacks/catalog.docker-compose.yml up --build
3. Setup BigQuery credentials: export TEST_BQ_CREDS=/path/to/bq_service_account.json
4. Run the tests from e2e directory, run: go test -v -timeout=30m ./...
5. To run a specific test do:
   go test -timeout 300s -testify.m "^(Test_Complete_QRep_Flow_Avro)$" .. The
   test name has to be changed accordingly. 