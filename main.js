// @ts-check

const { program } = require("commander");

const { MigrationClient } = require("./migration-client.js");
const { pause, parseParallelism, validate } = require("./utils.js");

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
      "-d, --timestamp <number>",
      "the timestamp from which to start syncing",
      parseInt,
    )
    .option(
      "-c, --collections <string...>",
      "[optional] the list of Collection names to be sync'ed",
    )
    .option(
      "-i, --indexes <string...>",
      "[optional] the list of Index names to be used with the respective Collections listed",
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

  if (!!options.collections && options.collections.length > 0) {
    if (
      !!options.indexes &&
      options.collections.length !== options.indexes.length
    ) {
      throw new Error(
        "Incorrect number of Indexes. The number of collections and indexes provided must be the same.",
      );
    }

    const collectionNames = options.collections;

    for (let i = 0; i < collectionNames.length; i++) {
      const collectionName = collectionNames[i];
      const indexName = !!options.indexes
        ? options.indexes[i]
        : "_migration_index_for_" + collectionName;

      collectionList.push({
        collectionName,
        indexName,
      });
    }
  } else {
    if (options.index) {
      throw new Error(
        "Custom indexes specified, but cannot be used when running with multiple collections.",
      );
    }
    console.log(
      "No collection specified. Gathering a list of all Collections in the source DB...",
    );

    const collectionNames = await migrator.listSourceCollections();

    for (const collectionName of collectionNames) {
      const indexName = "_migration_index_for_" + collectionName;
      collectionList.push({
        collectionName,
        indexName,
      });
    }
  }

  console.log(`Migrating ${collectionList.length} collections:`);
  for (const collection of collectionList) {
    const { collectionName, indexName } = collection;
    console.log(`  - ${collectionName} :: ${indexName}`);
  }
  console.log();

  if (options.validate) {
    console.log("Ready to Validate collections.");

    for (const collection of collectionList) {
      const { collectionName, indexName } = collection;

      console.log(`Validating migration of Collection("${collectionName}")`);

      const validationOptions = {
        ...options,
        collection: collectionName,
        index: indexName,
      };

      await validate(validationOptions);
    }
  } else {
    console.log("Ready to Synchronize collections.");

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

    console.log(`BEGIN synchronizing events at ${new Date().toISOString()}`);
    console.log();

    do {
      const startString = new Date(startTime / 1000).toISOString();
      const endString = new Date(
        startTime / 1000 + DURATION * 60 * 1000,
      ).toISOString();
      console.log(
        `Searching for events between ${startString} and ${endString}`,
      );
      console.log();

      for (const collection of collectionList) {
        const { collectionName, indexName } = collection;

        console.log(`Synchronizing Collection("${collectionName}")`);

        await migrator.migrateCollection({
          collectionName,
          indexName,
          startTime,
          duration: DURATION,
        });

        console.log(`Sleeping for ${WAIT_TIME} seconds`);
        await pause(WAIT_TIME * 1000); //sleep in ms
        console.log();
      }

      startTime += DURATION * 60 * 1000 * 1000; //increase the start time by 'DURATION' amount of minutes at every iteration
      iterations--;
    } while (iterations > 0 && startTime < Date.now() * 1000);

    console.log(`END synchronizing events at ${new Date().toISOString()}`);
  }
})().catch((err) => {
  console.error(err);
});
