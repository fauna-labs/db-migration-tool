// @ts-check

require("dotenv").config();
const { fql } = require("fauna");
const { runMigration } = require("../../migrate");
const { setup } = require("./setup");

const INTERVAL_SIZE = 500;
const INTERVAL_WAIT_MS = 1000;

const testMigration = async () => {
  // **************************************************************************
  // DB Setup
  // **************************************************************************

  console.log("Initializing the database...");

  const { sourceClient, sourceKey, targetClient, targetKey } = await setup();

  // **************************************************************************
  // Phase 1: Create initial source data and migrate to target
  // **************************************************************************

  console.log("Phase 1: Create initial source data and migrate to target");

  const phase1_start = await sourceClient
    .query(fql`Time.now().toMicros()`)
    .then((res) => res.data);
  console.log(`  Phase 1 start: ${phase1_start}`);

  for (let i = 0; i < 5; i++) {
    console.log(`  Creating interval ${i}`);

    const range = [...Array(INTERVAL_SIZE).keys()];

    const ts = await sourceClient
      .query(
        fql`
          let range = ${range}
          range.forEach(j =>
            C1.create({
              interval: ${i},
              value: j,
            })
          )
          Time.now().toMicros()
        `,
      )
      .then((res) => res.data);

    console.log(`  ts: ${ts}`);
    console.log(`  waiting for next interval...`);
    await new Promise((resolve) => setTimeout(resolve, INTERVAL_WAIT_MS));
  }

  const options = {
    source: sourceKey,
    target: targetKey,
    collection: "C1",
    timestamp: phase1_start,
    endpoint: "http://localhost:8443",
    parallelism: 10,
  };
  
  console.log("  Migrating data with options:");
  console.log(`    --source ${options.source} --target ${options.target} --collection ${options.collection} --endpoint ${options.endpoint} --timestamp ${options.timestamp}`);
  console.log()

  await runMigration(options);

  console.log("  Validating migration...");

  await runMigration({ ...options, validate: 500 });
};

testMigration();
