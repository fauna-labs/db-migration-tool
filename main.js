const { migrate } = require("./migrate-db.js");
const { lastProcessed } = require("./migrate-db.js");
const { program } = require("commander");

async function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
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
  .requiredOption('-s, --source <string>', 'access secret for the source DB')
  .requiredOption('-t, --target <string>', 'access secret for the target DB')
  .requiredOption('-c, --collection <string>', 'the name of the collection to be sync\'ed')
  .option('-i, --index <string>', 'the name of the index to use to sync the collection\'ed')
  .parse(process.argv);

  
  const options = program.opts();
  /*
  if (options.debug) console.log(options);
  console.log('key details:');
  if (options.source) console.log(`- ${options.source}`);
  */

  var index = options.index ? options.index : "_migration_index_for_" + options.collection;

  var startTime = Date.parse("2023-09-24T05:28:57Z") * 1000;
  var duration = 30; //fetch events for the time duration in minutes

  var size = 64; //page size

  var interval = 30; // time to pause between reading and applying events - in seconds

  var iterations = 20; // Define the total number of iterations

  lastProcessed.startTime = startTime;
  lastProcessed.updates.ts = startTime;
  lastProcessed.removes.ts = startTime;

  let res = await migrate(options.collection, index, duration, size, options.source, options.target );

  while (
    res &&
    iterations > 0 &&
    lastProcessed.startTime < Date.now() * 1000
  ) {
    lastProcessed.startTime += duration * 60 * 1000 * 1000; //increase the start time by 'duration' amount of minutes at every iteration

    await pause(interval * 1000).then(
      console.log(`Sleeping for ${interval} seconds`)
    ); //sleep in ms

    iterations--;

    res = await migrate(options.collection, index, duration, size, options.source, options.target );
  }

  if (!res) {
    throw new Error("Validation error; can't continue");
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
