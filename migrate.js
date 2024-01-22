// @ts-check

const { MigrationClient } = require("./migration-client.js");
const { pause, validate } = require("./utils.js");

// TUNABLE CONSTANTS
const DURATION = 30; // Time span (in minutes) to gather events
const ITERATIONS = 20; // Number of iterations to run the tool
const WAIT_TIME = 10; // Wait time between iterations in seconds
const DEFAULT_PAGE_SIZE = 64; // Page size for retrieving documents from the custom index

async function runMigration(options) {
  if (!options.source)
    throw new Error("Error running migration: 'source' option is required");
  if (!options.target)
    throw new Error("Error running migration: 'target' option is required");
  if (!options.timestamp)
    throw new Error("Error running migration: 'timestamp' option is required");
  if (!options.endpoint)
    throw new Error("Error running migration: 'endpoint' option is required");
  if (!options.parallelism)
    throw new Error(
      "Error running migration: 'parallelism' option is required",
    );

  try {
    const migrator = new MigrationClient({
      sourceKey: options.source,
      targetKey: options.target,
      defaultPageSize: DEFAULT_PAGE_SIZE,
      maxParallelism: options.parallelism,
      endpoint: options.endpoint,
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
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

module.exports = { runMigration };
