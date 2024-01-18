// @ts-check

const { MigrationClient } = require("./migration-client.js");
const { pause, validate } = require("./utils.js");

// TUNABLE CONSTANTS
const DURATION = 30; // Time span (in minutes) to gather events
const ITERATIONS = 20; // Number of iterations to run the tool
const WAIT_TIME = 10; // Wait time between iterations in seconds
const DEFAULT_PAGE_SIZE = 64; // Page size for retrieving documents from the custom index

async function runMigration(options) {
  if (!options.source) throw new Error("Error running migration: 'source' option is required")
  if (!options.target) throw new Error("Error running migration: 'target' option is required")
  if (!options.collection) throw new Error("Error running migration: 'collection' option is required")
  if (!options.timestamp) throw new Error("Error running migration: 'timestamp' option is required")
  if (!options.endpoint) throw new Error("Error running migration: 'endpoint' option is required")
  if (!options.parallelism) throw new Error("Error running migration: 'parallelism' option is required")

  try {
    if (options.validate) {
      await validate(options);
    } else {
      const collectionName = options.collection;
      const indexName =
        options.index ?? "_migration_index_for_" + collectionName;

      const migrator = new MigrationClient({
        sourceKey: options.source,
        targetKey: options.target,
        defaultPageSize: DEFAULT_PAGE_SIZE,
        maxParallelism: options.parallelism,
        endpoint: options.endpoint,
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
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

module.exports = { runMigration };
