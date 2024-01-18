// @ts-check

const { Client, fql } = require("fauna");
const { SchemaClient } = require("../schema_client");
const { MigrationClient } = require("../../migration-client");

const endpoint = "http://localhost:8443";

const SCHEMA = `
  collection C1 { history_days 1 }
`;

const setup = async () => {
  const root_client = new Client({
    secret: "secret",
    endpoint: new URL(endpoint),
  });

  try {

    /**  @type {{data: [string, string]}} */
    const createDBsResponse = await root_client.query(fql`
      let sourceName = "migrate_source_".concat(newId().toString())
      let targetName = "migrate_target_".concat(newId().toString())
  
      Database.create({ name: sourceName })
      Database.create({ name: targetName })
  
      [sourceName, targetName]
    `);

    const [sourceName, targetName] = createDBsResponse.data;

    /**  @type {{data: [string, string]}} */
    const createKeysResponse = await root_client.query(fql`
      let source_key = Key.create({ database: ${sourceName}, role: "admin" })
      let target_key = Key.create({ database: ${targetName}, role: "admin" })
  
      [source_key.secret, target_key.secret]
    `);
    const [sourceKey, targetKey] = createKeysResponse.data;

    // Initialize schema

    const sourceSchemaClient = new SchemaClient({
      secret: sourceKey,
      endpoint: endpoint,
    });

    const targetSchemaClient = new SchemaClient({
      secret: targetKey,
      endpoint: endpoint,
    });

    await sourceSchemaClient.update("main.fsl", SCHEMA);
    await targetSchemaClient.update("main.fsl", SCHEMA);

    const sourceClient = new Client({
      secret: sourceKey,
      endpoint: new URL(endpoint),
    });
    const targetClient = new Client({
      secret: targetKey,
      endpoint: new URL(endpoint),
    });

    const migrationClient = new MigrationClient({
      sourceKey,
      targetKey,
      defaultPageSize: 64,
      maxParallelism: 10,
      endpoint
    });

    console.log("Preparing collections for migration...");

    await migrationClient.initializeSourceFunctions();
  
    // do this now before there is any data in the collections and the indexes
    // should be active right away
    let initialized = false;
    do {
      initialized = await migrationClient.initializeCollection({
        collectionName: "C1",
        indexName: "_migration_index_for_C1",
      });
    } while (!initialized);

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
