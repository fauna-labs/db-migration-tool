// @ts-check

const { MigrationClient } = require("./migration-client.js");
const { pause, parseParallelism, validate } = require("./utils.js");
const { program } = require("commander");

// TUNABLE CONSTANTS
const DURATION = 200; // Time span (in minutes) to gather events
const ITERATIONS = 20; // Number of iterations to run the tool
const WAIT_TIME = 1; // Wait time between iterations in seconds
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
    .option(
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

  const migrator = new MigrationClient({
    sourceKey: options.source,
    targetKey: options.target,
    defaultPageSize: DEFAULT_PAGE_SIZE,
    maxParallelism: options.parallelism,
  });

  const collectionList = [];
  if (options.collection) {
    const collectionName = options.collection;
    const indexName = options.index ?? "_migration_index_for_" + collectionName;
    collectionList.push({
      collectionName,
      indexName,
    });
  } else {
    if (options.index) {
      console.log(
        "Custom index specified, but cannot be used when running with multiple collections.",
      );
    }
    console.log(
      "No collection specified. Gathering a list of all Collections in the source DB...",
    );

    const collectionNames = await migrator.listSourceCollections();

    console.log(`Found ${collectionNames.length} collections:`);
    console.log(collectionNames);
    for (const collectionName of collectionNames) {
      console.log(`  - ${collectionName}`);

      collectionList.push({
        collectionName,
        indexName: "_migration_index_for_" + collectionName,
      });
    }
  }

  if (options.validate) {
    for (const collection of collectionList) {
      const { collectionName, indexName } = collection;

      const validationOptions = {
        ...options,
        collection: collectionName,
        index: indexName,
      };

      console.log(`Validating complete migration of Collection("${collectionName}")`);
      await validate(validationOptions);
    }
  } else {
    console.log();
    console.log("Initializing UDFs...");
    await migrator.initializeSourceFunctions();
    console.log();

    console.log(`Initializing Collections...`);
    let collectionsInitialized = true;
    for (const collection of collectionList) {
      const { collectionName, indexName } = collection;

      const initialized = await migrator.initializeCollection({
        collectionName,
        indexName,
      });

      collectionsInitialized = collectionsInitialized && initialized;
    }
    if (!collectionsInitialized) {
      throw new Error("Initialization error; can't continue");
    }
    console.log();

    let iterations = ITERATIONS;
    let startTime = options.timestamp;

    do {
      console.log(
        `BEGIN synchronizing events at ${new Date().toISOString()}...`,
      );
      console.log();

      for (const collection of collectionList) {
        const { collectionName, indexName } = collection;

        console.log(`Synchronizing Collection("${collectionName}")...`)

        await migrator.migrateCollection({
          collectionName,
          indexName,
          startTime,
          duration: DURATION,
        });

        console.log(`Sleeping for ${WAIT_TIME} seconds`)
        await pause(WAIT_TIME * 1000) //sleep in ms
        console.log()
      }

      startTime += DURATION * 60 * 1000 * 1000; //increase the start time by 'DURATION' amount of minutes at every iteration
      iterations--;

      console.log(
        `END synchronizing events at ${new Date().toISOString()}...`,
      );
    } while (iterations > 0 && startTime < Date.now() * 1000);
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
