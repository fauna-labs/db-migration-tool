const { migrate } = require("./migrate-db.js");
const { lastProcessed } = require("./migrate-db.js");

async function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
(async () => {
  // Start the program - first time
  // Provide the startTime, collection, index and how many minutes of writes the destination db needs to catch up each time. 
  // Number of iterations are configurable
  // With duration=30 and iteration=10, it migrates  30 mins worth of data from source db and does this 10 times, 
  // waiting 2 mins (interval=120) between each fetch 

  var startTime = Date.parse("2023-10-27T23:06:18Z") * 1000;
  var coll = "collectioName"; //collection name
  var index = "indexName"; //index name
  var duration = 30; //fetch events for the time duration in minutes

  var size = 64; //page size

  var interval = 30; // time to pause between reading and applying events - in seconds

  var iterations = 20; // Define the total number of iterations

  lastProcessed.startTime = startTime;
  lastProcessed.updates.ts = startTime;
  lastProcessed.removes.ts = startTime;

  let res = await migrate(coll, index, duration, size);

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

    res = await migrate(coll, index, duration, size);
  }

  if (!res) {
    throw new Error("Validation error; can't continue");
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
