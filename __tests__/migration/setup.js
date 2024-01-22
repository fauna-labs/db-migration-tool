// @ts-check

const { Client, query: q } = require("faunadb");
const { MigrationClient } = require("../../migration-client");

const endpoint = "http://localhost:8443";

const setup = async (collectionNames) => {
  const rootClient = new Client({
    secret: "secret",
    endpoint: endpoint,
  });

  try {
    /**  @type {[string, string]} */
    const createDBsResponse = await rootClient.query(
      q.Let(
        {
          sourceName: q.Concat(["migrate_source_", q.NewId()]),
          targetName: q.Concat(["migrate_target_", q.NewId()]),
          source: q.CreateDatabase({ name: q.Var("sourceName") }),
          target: q.CreateDatabase({ name: q.Var("targetName") }),
        },
        [q.Var("sourceName"), q.Var("targetName")],
      ),
    );
    const [sourceName, targetName] = createDBsResponse;

    /**  @type {[string, string]} */
    const createKeysResponse = await rootClient.query(
      q.Let(
        {
          source_key: q.CreateKey({
            database: q.Database(sourceName),
            role: "admin",
          }),
          target_key: q.CreateKey({
            database: q.Database(targetName),
            role: "admin",
          }),
        },
        [
          q.Select("secret", q.Var("source_key")),
          q.Select("secret", q.Var("target_key")),
        ],
      ),
    );
    const [sourceKey, targetKey] = createKeysResponse;

    const sourceClient = new Client({
      secret: sourceKey,
      endpoint: endpoint,
    });
    const targetClient = new Client({
      secret: targetKey,
      endpoint: endpoint,
    });

    const createCollectionsQuery = q.Map(collectionNames, (name) =>
      q.CreateCollection({ name, history_days: 1 }),
    );

    await sourceClient.query(createCollectionsQuery);
    await targetClient.query(createCollectionsQuery);

    const migrationClient = new MigrationClient({
      sourceKey,
      targetKey,
      defaultPageSize: 64,
      maxParallelism: 10,
      endpoint,
    });

    console.log("Preparing collections for migration...");

    await migrationClient.initializeSourceFunctions();

    // do this now before there is any data in the collections and the indexes
    // should be active right away
    let initialized = false;
    for (const collectionName of collectionNames) {
      do {
        initialized = await migrationClient.initializeCollection({
          collectionName,
          indexName: `_migration_index_for_${collectionName}`,
        });
      } while (!initialized);
    }

    return {
      sourceClient,
      sourceKey,
      targetClient,
      targetKey,
    };
  } catch (e) {
    throw new Error("Setup failed", { cause: e });
  }
};

module.exports = { setup };
