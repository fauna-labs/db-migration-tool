# db-migration-tool

This tool can be used to migrate a database from one RG to a different RG.
It uses Fauna's temporality feature to capture all events for each collection in a database that occurred after a given timestamp, and plays them back in the same order on the destination RG.
The tool works in combination with the Copy feature. By itself, it does not copy over or migrate indexes or keys.
It works on each collection in a database.
If it doesn't already exist, an index of the following shape will be created:
  `{ name: "<index-name>", source: Collection("<collection-name>"), terms: [], values: [ { field: "ts" }, { field: "ref" } ] }`

Pre-requisites

- Take a snapshot of the source database
- Enable history_days on all collections that need to be migrated
- This index needs to be active before the data can be migrated

Steps:

1. Create the UDFs `get_events_from_collection` and `get_remove_events_from_collection` in the source database(A)
2. Create a database(B) in the target RG from the latest available snapshot of the database A.
3. Generate admin key for database B
4. Run the script in `main.js`
   - The script takes three required arguments: the access secret for the source database, the access secret for the target database, and the name of the collection you want to sync
   - Specify the timestamp, and desired duration in main.js
     
Note: Any new schema documents (collections, indexes) created after the snapshot was copied will not be migrated.
