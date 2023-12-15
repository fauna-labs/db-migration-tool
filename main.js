// @ts-check

const { MigrationClient } = require("./migration-client.js");
const { pause, parseParallelism, validate } = require("./utils.js");
const { program } = require("commander");

// TUNABLE CONSTANTS
const DURATION = 30; // Time span (in minutes) to gather events
const ITERATIONS = 20; // Number of iterations to run the tool
const WAIT_TIME = 10; // Wait time between iterations in seconds
const DEFAULT_PAGE_SIZE = 64; // Page size for retrieving documents from the custom index

(async () => {
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
    .parse(process.argv);

  const options = program.opts();

  if (options.validate) {
    await validate(options);
  } else {
    const collectionName = options.collection;
    const indexName = options.index ?? "_migration_index_for_" + collectionName;

    const migrator = new MigrationClient({
      sourceKey: options.source,
      targetKey: options.target,
      defaultPageSize: DEFAULT_PAGE_SIZE,
      maxParallelism: options.parallelism,
    });

    console.log("Initializing UDFs...");
    await migrator.initializeSourceFunctions();

    console.log(`Initializing Collection '${collectionName}'...`);
    const initialized = await migrator.initializeCollection({
      collectionName,
      indexName,
    });
    if (!initialized) {
      throw new Error("Initialization error; can't continue");
    }

    console.log(
      `BEGIN synchronizing events in collection '${collectionName}' at ${new Date().toISOString()}...`,
    );

    let iterations = ITERATIONS;
    let startTime = options.timestamp;

    do {
      await migrator.migrateCollection({
        collectionName,
        indexName,
        startTime,
        duration: DURATION,
      });

      startTime += DURATION * 60 * 1000 * 1000; //increase the start time by 'DURATION' amount of minutes at every iteration

      await pause(WAIT_TIME * 1000).then(() =>
        console.log(`Sleeping for ${WAIT_TIME} seconds`),
      ); //sleep in ms

      iterations--;
    } while (iterations > 0 && startTime < Date.now() * 1000);

    console.log(
      `END synchronizing events in collection '${
        options.collection
      }' at ${new Date().toISOString()}`,
    );
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
