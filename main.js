// @ts-check

const { MigrationClient } = require("./migration-client.js");
const { pause, parseParallelism, validate } = require("./utils.js");
const { program } = require("commander");
const { runMigration } = require("./migrate.js");

// Start the program - first time
// Provide the startTime, collection, index and how many minutes of writes the destination db needs to catch up each time.
// Number of iterations are configurable
// With DURATION=30 and ITERATIONS=10, it migrates 30 mins worth of data from source db and does this 10 times,
// waiting WAIT_TIME seconds between each iteration.

program
  .name("fauna-db-sync")
  .description("migrates lastest writes from one DB to another")
  .version("0.0.0", "-v, --version")
  .usage("[OPTIONS]...")
  .requiredOption("-s, --source <string>", "admin secret for the source DB")
  .requiredOption("-t, --target <string>", "admin secret for the target DB")
  .requiredOption(
    "-c, --collection <string>",
    "the name of the collection to be sync'ed",
  )
  .requiredOption(
    "-d, --timestamp <number>",
    "the timestamp from which to start syncing",
    parseInt,
  )
  .option(
    "-i, --index <string>",
    "[optional] the name of the index to use to sync the collection",
  )
  .option(
    "-p, --parallelism <number>",
    "[optional] apply up to N events per transaction",
    parseParallelism,
    10,
  )
  .option(
    "--validate <number>",
    "[optional] paginate through documents N at a time (1000 max) and compare source to target; WARNING: this could take a long time and will accrue additional read ops",
    parseInt,
  )
  .option(
    "--endpoint <string>",
    "[optional] the endpoint to use for the source and target DBs",
    "https://db.fauna.com",
  )
  .parse(process.argv);

const options = program.opts();

runMigration(options);