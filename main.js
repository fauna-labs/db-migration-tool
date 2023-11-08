const { migrate, initialize } = require("./migrate-db.js");
const { lastProcessed } = require("./migrate-db.js");
const { pause, parseParallelism, validate } = require("./utils.js");
const { program } = require("commander");

(async () => {
  // Start the program - first time
  // Provide the startTime, collection, index and how many minutes of writes the destination db needs to catch up each time. 
  // Number of iterations are configurable
  // With duration=30 and iteration=10, it migrates  30 mins worth of data from source db and does this 10 times, 
  // waiting 2 mins (interval=120) between each fetch 
  program
    .name("fauna-db-sync")
    .description('migrates lastest writes from one DB to another')
    .version('0.0.0', '-v, --version')
    .usage('[OPTIONS]...')
    .requiredOption('-s, --source <string>', 'admin secret for the source DB')
    .requiredOption('-t, --target <string>', 'admin secret for the target DB')
    .requiredOption('-c, --collection <string>', 'the name of the collection to be sync\'ed')
    .requiredOption('-d, --timestamp <number>', 'the timestamp from which to start syncing', parseInt)
    .option('-i, --index <string>', '[optional] the name of the index to use to sync the collection')
    .option('-p, --parallelism <number>', '[optional] apply up to N events per transaction', parseParallelism, 10)
    .option('--validate <number>', '[optional] paginate through documents N at a time (1000 max) and compare source to target; WARNING: this could take a long time and will accrue additional read ops', parseInt)
    .parse(process.argv);
  
  const options = program.opts();

  if (options.validate) {
    await validate(options);
  } else {
    var index = options.index ?? "_migration_index_for_" + options.collection;

    // TUNABLE CONSTANTS
    var duration = 30; //fetch events for the time duration in minutes
    var size = 64; //page size
    var interval = 30; // time to pause between reading and applying events - in seconds
    var iterations = 20; // Define the total number of iterations

    lastProcessed.startTime = options.timestamp;
    lastProcessed.updates.ts = options.timestamp;
    lastProcessed.removes.ts = options.timestamp;

    console.log(`BEGIN synchronizing events in collection '${options.collection}' at ${new Date().toISOString()}`);

    let initialized = await initialize(options.collection, index, options.source, options.target)
    if (!initialized) {
      throw new Error("Initialization error; can't continue")
    }

    await migrate(options.collection, index, duration, size, options.parallelism);

    while (
      iterations > 0 &&
      lastProcessed.startTime < Date.now() * 1000
    ) {
      lastProcessed.startTime += duration * 60 * 1000 * 1000; //increase the start time by 'duration' amount of minutes at every iteration

      await pause(interval * 1000).then(
        console.log(`Sleeping for ${interval} seconds`)
      ); //sleep in ms

      iterations--;

      await migrate(options.collection, index, duration, size, options.parallelism);
    }

    console.log(`END synchronizing events in collection '${options.collection}' at ${new Date().toISOString()}`);
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
