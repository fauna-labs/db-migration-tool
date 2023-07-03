# db-migration-tool

This tool can be used to migrate a database from one RG to a different RG. 
It uses Fauna's temporality feature to capture all events for each collection in a database that occurred after a given timestamp, and plays them back in the same order on the destination RG.
The tool works in combination with the Copy feature. By itself, it does not copy over or migrate indexes or keys.
It works at the database level. 

Pre-requisites
- Take a snapshot of the source database
- Enable history_days on all collections that need to be migrated

Steps:
1. Create the UDFs `get_all_events`, `get_events_from_collection` and `get_remove_events` in the source database(A)
2. Create a database(B) in the target RG from the latest available snapshot of the database A.
3. Generate admin key for database B
4. Run the script `migrate-db.js`
   - Specify the correct secret for each database in the client configuration.
   - Specify the timestamp on line 106 - all events after this timestamp will be copied over.

Note: Any new schema documents (collections, indexes) created after the snapshot was copied will not be migrated. 
