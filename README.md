# db-migration-tool

This tool can be used to migrate a database from one RG to a different RG.
It uses Fauna's temporality feature to capture all events for each collection in a database that occurred after a given timestamp, and plays them back in the same order on the destination RG.
The tool works in combination with the Copy feature. By itself, it does not copy over or migrate indexes or keys.
It works on each collection in a database.

Pre-requisites

- Take a snapshot of the source database
- Enable history_days on all collections that need to be migrated
- Create an Index on each collection that needs to be migrated with the following definition
  `{ name: "<index-name>", source: Collection("<collection-name>"), terms: [], values: [ { field: "ts" }, { field: "ref" } ] }`
- This index needs to be active before the data can be migrated

Steps:

1. Create the UDFs `get_events_from_collection` and `get_remove_events_from_collection` in the source database(A)
2. Create a database(B) in the target RG from the latest available snapshot of the database A.
3. Generate admin key for database B
4. Run the script in `main.js`
   - Specify the timestamp, collection name, index name, desired duration in `main.js`
   - Specify the correct secret for each database in the client configuration in `migrate-db.js`
     
Note: Any new schema documents (collections, indexes) created after the snapshot was copied will not be migrated.
