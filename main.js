import { migrate } from "./migrate-db.js";
import { lastProcessed } from "./migrate-db.js";


async function scheduleMigrateQuery(iterations) {
  
  lastProcessed.startTime += duration * 60 * 1000 * 1000; //increase the start time by 'duration' amount of minutes at every iteration

  //exit condition
  if (
    iterations <= 0 ||
    lastProcessed.startTime > Date.parse(new Date(Date.now())) * 1000 //if startTime goes beyond the current time, exit.
  ) {
    process.exit();
  }

  await migrate(coll, index, duration, size);

  await pause(interval * 1000).then(
    console.log("Sleeping now ", new Date(Date.now()))
  ); //sleep in ms

  console.log("awake now: ", new Date(Date.now()));
  await scheduleMigrateQuery(iterations - 1);
}

// Start the program - first time
//Priovide the startTime, collection, index and how many minutes of writes the destination db needs to catch up each time. 
//Number of iterations are configurable
//With duration=30 and iteration=10, it migrates  30 mins worth of data from source db and does this 10 times, 
//waiting 2 mins (interval=120) between each fetch 

var startTime = Date.parse("2023-09-24T05:28:57Z") * 1000;
var coll = "Book"; //collection name
var index = "Book_Events"; //index name
var duration = 30; //fetch events for the time duration in minutes

var size = 64; //page size

var interval = 120; // time to pause between reading and applying events - in seconds

var iterations = 10; // Define the total number of iterations

lastProcessed.startTime = startTime;
lastProcessed.updates.ts = startTime;
lastProcessed.removes.ts = startTime;

const res = await migrate(coll, index, duration, size);

if (res) {
  await scheduleMigrateQuery(iterations);
}

async function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
