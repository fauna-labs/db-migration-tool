# db-migration-tool

This tool can be used to migrate the data in a given collection from a source database in one Fauna Region Group to another target database, which can be in an entirely different Region Group.
It uses Fauna's temporality feature to replicate all the write events that occurred after a given timestamp in the source collection, into the same collection of a target database. This tool can be use in combination with [Fauna's Datbase Copy](https://docs.fauna.com/fauna/current/administration/backups#create-a-database-from-a-snapshot) feature to achieve a database migration. By itself, this tool does not copy over or migrate indexes or keys.

The general procedure for migrating from one Region Group to another is to create a backup snapshot of the source database, create a new database from that snapshot on the desired Region Group. Once the database copy is complete, use this tool to synchronize the writes that have occurred on the sources database since the snapshot was taken. More guidance regarding migration is included at the end of this README.

## Pre-requisites

### Installation

Install [Node.js](https://nodejs.org/en)

Verify Node.js installation

```shell
$ node --version
```

Verify npm installation

```shell
$ npm --version
```

Clone the repository

```shell
$ cd to/my/directory
$ git clone https://github.com/fauna-labs/db-migration-tool.git
```

### Prepare your databases

1. Enable `history_days` on all collections that need to be migrated.
   1. Update each collection's [document](https://docs.fauna.com/fauna/current/reference/schema_entities/collection/document_definition#fields) as required.
      > [!IMPORTANT]
      > To avoid gaps in data, be sure to set `history_days` to a period greater than the time expected to complete the migration. For example, if you plan to take 3 days from the time of snapshot to complete the migration, then set `history_days` to a value greater than 3.
2. Schedule a snapshot of the source databases.
   1. Please refer to the [Backups documentation](https://docs.fauna.com/fauna/current/administration/backups) for more information.

## Using the tool

### Running the script

Open a terminal, navigate to the project directory, and execute the `main.js` file with necessary options.

Example:
```shell
$ cd to/my/directory/db-migration-tool
$ node main.js --source $SOURCE_KEY --target $TARGET_KEY -c Customer -d 1698969600000000
```

CLI Option:
```
Options:
  -v, --version               output the version number
  -s, --source <string>       admin secret for the source DB
  -t, --target <string>       admin secret for the target DB
  -c, --collection <string>   the name of the collection to be sync'ed
  -d, --timestamp <string>    the timestamp from which to start syncing
  -i, --index <string>        the name of the index to use to sync the collection
  -p, --parallelism <number>  apply up to N events per transaction (default: 10)
  -h, --help                  display help for command
```

### Indexes

This tool will automatically create a new index in the source collection, that is necessary to properly sync data from the source collection. These autogenerated indexes vary on how long it takes for their build process to complete, which depends on the size of the collection. The index needs to be active before this tool can sync the collection data.

Indexes of the following shape will be created:

```javascript
{
  name: "IndexName",
  source: Collection("CollectionName"),
  terms: [],
  values: [ { field: "ts" }, { field: "ref" } ]
}
```

### Converting between ISO time strings and Timestamps

A Fauna Timestamp, an integer representing the microseconds since the epoch, is needed to define the start of the synchronization operation (passed in with the `-d, --timestamp` option). You can use Fauna to convert between ISO time strings and Timestamps.

String to Timestamp
```javascript
// FQL v4
ToMicros(Time("2023-11-03T00:00:00Z")) // 1698969600000000
```

Timestamp to String
```javascript
// FQL v4
Epoch(1698969600000000, "microseconds") // Time("2023-11-03T00:00:00Z")
```

### Best Practices
- To avoid gaps in synchronization, you should use a start timestamp less than the timestamp of the last synced write on the target collection.
- To reduce the overall time to sync an entire database, run one instance of this tool for each collection, in parallel.

## Process for Migration

### 1. Copy your database to the desired Region Group

1. Check that all prerequisites listed earlier in this README have been met.
2. Create a database copy (target database) in the target RG from the latest available snapshot of source database.

### 2. Synchronization

> [!NOTE]
> If you paused writes to your database before the snapshot time, this script is not needed to because no data needs to be synchronized. You can skip to Application cutover.

The script is [idempotent](https://en.wikipedia.org/wiki/Idempotence), so it can safely be run multiple times. We recommend running the script at least once before pausing writes for your application to cutover. This gives you the chance to monitor the time it takes to perform the sync operation and plan ahead.

Monitor the time it takes to perform this update, as this is the theoretical minimum downtime that can be achieved during cutover. You can run this update on a regular basis to get an typical baseline of the time needed to sync the database with the latest writes since the tool was last run.

1. Generate admin keys for the source database and target database.
2. Run the script in `main.js`, providing the required authentication keys, the name of the collection to be sync'ed, and the timestamp from which write history is to be applied to the target database.
   1. The first time you run the script, the timestamp should be a time before the snapshot was taken. This will sync all the writes that occurred after the snapshot was taken.
   2. Subsequent executions of the script can use the timestamp of the last successful execution. This will sync all the writes that occurred since the last successful execution.
3. Wait for the required indexes to become active. The script will stop if the index is not active and prompt you to try again later. Once the indexes are active, the script will procede to sync the collection data.
4. Repeat the operation for all collections in all databases that need to be migrated.

### 3. Application cutover

  > [!IMPORTANT]
  > It is Recommended that this be scheduled for a window when the downtime least impacts your application workload

Application cutover is the action of transitioning your application from using the source database and reconnecting it to the target database. The strategy for your cutover involves changing the access keys your application uses to connect to the Fauna. Application cutover occurs when you have replaced the keys which connect your application to the source database with keys that connect to the target database.

1. Disable writes from the application.
2. Confirm no new writes are occurring.
3. Run a final execution of this tool to sync the latest writes to the target database. 
4. Update access keys in the application with keys pointing to the target database.
5. Reenable writes in your application


## Limitations

- Documents with over 100,000 events will only have the first set of 100,000 events copied over.
- Any new schema documents (collections, indexes) created after the snapshot was copied will not be migrated. Usage of this tool is not recommended while schema documents are modified.
- Creates, updates, and deletes applied after the snapshot was taken will be copied in order by this script but using the current time. In other words, the `ts` field's value is not preserved.
- Usage of history manipulation is incompatible with this script. Because this script only looks at events in time order going forward, it will miss events manipulated in the past.
