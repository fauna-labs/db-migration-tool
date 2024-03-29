// @ts-check

require("dotenv").config();
const { query: q } = require("faunadb");
const { runMigration } = require("../../migrate");
const { setup } = require("./setup");

const NUM_INTERVALS = 5;
const INTERVAL_SIZE = 500;
const INTERVAL_WAIT_MS = 1000;

const COLLECTION_NAMES = ["C1", "C2", "C3"];

const testMigration = async () => {
  // **************************************************************************
  // DB Setup
  // **************************************************************************

  console.log("Initializing the database...");

  const { sourceClient, sourceKey, targetClient, targetKey } =
    await setup(COLLECTION_NAMES);

  // **************************************************************************
  // Phase 1: Create initial source data and migrate to target
  // **************************************************************************

  console.log("Phase 1: Create initial source data and migrate to target");

  /** @type {number} */
  const phase1_start = await sourceClient.query(
    q.ToMicros(q.TimeSubtract(q.Now(), NUM_INTERVALS, "hour")),
  );
  console.log(`  Phase 1 start: ${phase1_start}`);

  for (const collectionName of COLLECTION_NAMES) {
    console.log(`  Creating docs for Collection(${collectionName})`);
    for (let i = 0; i < NUM_INTERVALS; i++) {
      console.log(`    Creating interval ${i}`);

      const range = [...Array(INTERVAL_SIZE).keys()];

      const ts = await sourceClient.query(
        q.Do(
          q.Map(range, (j) =>
            q.Insert(
              q.Ref(q.Collection(collectionName), q.NewId()),
              phase1_start + 60 * 60 * 1000 * 1000 * i,
              "create",
              {
                data: {
                  interval: i,
                  value: j,
                  total: q.Add(q.Multiply(i, INTERVAL_SIZE), j),
                },
              },
            ),
          ),
          q.ToMicros(q.Now()),
        ),
      );

      console.log(`    ts: ${ts}`);
      console.log(`    waiting for next interval...`);
      await new Promise((resolve) => setTimeout(resolve, INTERVAL_WAIT_MS));
    }
  }

  const options = {
    source: sourceKey,
    target: targetKey,
    timestamp: phase1_start,
    endpoint: "http://localhost:8443",
    parallelism: 10,
    yes: true,
  };

  console.log("  Migrating data with options:");
  console.log(
    `    --source ${options.source} --target ${options.target} --endpoint ${options.endpoint} --timestamp ${options.timestamp}`,
  );
  console.log();

  await runMigration(options);

  console.log();
  console.log("  Validating migration...");

  await runMigration({ ...options, validate: 500 });
  console.log();

  // **************************************************************************
  // Phase 2: Do some updates and deletes, then migrate to target
  // **************************************************************************

  console.log("Phase 2: Do some updates and deletes, then migrate to target");

  /** @type {number} */
  const phase2_start = await sourceClient.query(q.ToMicros(q.Now()));
  console.log(`  Phase 2 start: ${phase2_start}`);

  const phase2CollectionNames = COLLECTION_NAMES.slice(0, 2);

  for (const collectionName of phase2CollectionNames) {
    // update 10% of the docs and delete another 10% of docs
    await sourceClient.query(
      q.Map(
        q.Paginate(q.Documents(q.Collection(collectionName)), { size: 100000 }),
        (ref) =>
          q.Let(
            { total: q.Select(["data", "total"], q.Get(ref)) },
            q.If(
              q.Equals(q.Modulo(q.Var("total"), 10), 1),
              q.Update(ref, { data: { updated: "updated" } }),
              q.If(
                q.Equals(q.Modulo(q.Var("total"), 10), 2),
                q.Delete(ref),
                null,
              ),
            ),
          ),
      ),
    );
  }

  const options2 = {
    ...options,
    timestamp: phase2_start,
    collections: phase2CollectionNames,
  };

  console.log("  Migrating data with options:");
  console.log(
    `    --source ${options2.source} --target ${
      options2.target
    } --collection ${phase2CollectionNames.join(" ")} --endpoint ${
      options2.endpoint
    } --timestamp ${options2.timestamp}`,
  );
  console.log();

  await runMigration(options2);

  console.log();
  console.log("  Validating migration...");

  await runMigration({ ...options2, validate: 500 });
};

testMigration();
