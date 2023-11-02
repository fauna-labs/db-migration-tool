# db-migration-tool

This tool can be used to migrate a database from one RG to a different RG.
It uses Fauna's temporality feature to capture all events for each collection in a database that occurred after a given timestamp, and plays them back in the same order on the destination RG.
The tool works in combination with the Copy feature. By itself, it does not copy over or migrate indexes or keys.
It works on each collection in a database.
If it doesn't already exist, an index of the following shape will be created:
  `{ name: "<index-name>", source: Collection("<collection-name>"), terms: [], values: [ { field: "ts" }, { field: "ref" } ] }`

## Pre-requisites

- Take a snapshot of the source database. Please refer to the [Backups documentation](https://docs.fauna.com/fauna/current/administration/backups) for more information.
- Enable history_days on all collections that need to be migrated. This is done by altering the each collection's [document](https://docs.fauna.com/fauna/current/reference/schema_entities/collection/document_definition#fields).
- The provided index needs to be active before the data can be migrated.

## Steps

1. Create the UDFs `get_events_from_collection` and `get_remove_events_from_collection` in the source database(A)
2. Create a database(B) in the target RG from the latest available snapshot of the database A.
3. Generate admin key for database B
4. Run the script in `main.js`
   - The script takes four required arguments: 
   -- the access secret for the source database, 
   -- the access secret for the target database, 
   -- the name of the collection you want to sync
   -- and the transaction timestamp at which the sync is to run from  

## Limitations

- Documents with over 100,000 events will only have the first set of 100,000 events copied over.
- Any new schema documents (collections, indexes) created after the snapshot was copied will not be migrated. Usage of this tool is not recommended while schema documents are modified.
- Creates, updates, and deletes applied after the snapshot was taken will be copied in order by this script but using the current time. In other words, the `ts` field's value is not preserved.
- Usage of history manipulation is incompatible with this script. Because this script only looks at events in time order going forward, it will miss events manipulated in the past.
