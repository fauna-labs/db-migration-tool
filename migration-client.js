// @ts-check

const fauna = require("faunadb");
const {
  getEventsFromCollectionFunctionQuery,
  getRemoveEventsFromCollectionFunctionQuery,
} = require("./functions.js");

const {
  At,
  Let,
  Get,
  Var,
  Index,
  Call,
  Exists,
  If,
  Create,
  Update,
  Delete,
  Collection,
  Select,
  LT,
  CreateIndex,
  Do,
  IsNull,
} = fauna.query;

const GET_EVENTS_FROM_COLLECTION = "_get_events_from_collection";
const GET_REMOVE_EVENTS_FROM_COLLECTION = "_get_remove_events_from_collection";

class MigrationClient {
  #sourceClient;
  #targetClient;
  #defaultPageSize;
  #maxParallelism;

  constructor({ sourceKey, targetKey, defaultPageSize, maxParallelism }) {
    this.#sourceClient = new fauna.Client({
      secret: sourceKey,
    });
    this.#targetClient = new fauna.Client({
      secret: targetKey,
    });
    this.#defaultPageSize = defaultPageSize;
    this.#maxParallelism = maxParallelism;
  }

  async initializeSourceFunctions() {
    await this.#ensureGetEventsFromCollection();
    await this.#ensureGetRemoveEventsFromCollection();
  }

  async initializeCollection({ collectionName, indexName }) {
    // validate that the given collection exists and that history_days is set
    const valColl = await this.#validateCollection(collectionName);
    if (typeof valColl != "number") {
      console.log(valColl);
      return false;
    }

    // verify that the index and functions exist, and if not then create them
    const indexActive = await this.#ensureIndex({ collectionName, indexName });
    // only continue if the index is active
    if (!indexActive) {
      console.log(`Index '${indexName}' is not active, retry later.`);
      return false;
    }

    // validate that the collection exists on destination db
    const valTargetColl = await this.#validateTargetCollection(collectionName);
    if (typeof valTargetColl == "string") {
      console.log(valTargetColl);
      return false;
    }

    return true;
  }

  async migrateCollection({ collectionName, indexName, startTime, duration }) {
    console.log();

    const startString = new Date(startTime / 1000).toISOString();
    const endString = new Date(
      startTime / 1000 + duration * 60 * 1000,
    ).toISOString();

    console.log(`Searching for events between ${startString} and ${endString}`);

    const docEvents = await this.#getAllEvents({
      indexName,
      startTime,
      duration,
    });

    const collEvents = await this.#getRemoveEvents({
      collectionName,
      startTime,
      duration,
    });

    await this.#applyEvents(docEvents, collEvents);
  }

  // ***************************************************************************
  // Private methods
  // ***************************************************************************

  async #validateCollection(collectionName) {
    const qry = If(
      Exists(Collection(collectionName)),
      Let(
        {
          h_days: Select("history_days", Get(Collection(collectionName))),
        },
        If(
          LT(0, Var("h_days")),
          Var("h_days"),
          "Please enable history for this collection",
        ),
      ),
      "Please make sure the collection exists on the source DB",
    );

    const res = await this.#sourceClient.query(qry).catch((err) => {
      console.error("validateCollection error: %s", err);
      throw err;
    });

    return res;
  }

  async #ensureIndex({ collectionName, indexName }) {
    // If the index doesn't exist, create it.
    const check_index_query = If(
      Exists(Index(indexName)),
      true,
      CreateIndex({
        name: indexName,
        source: Collection(collectionName),
        values: [{ field: ["ts"] }, { field: ["ref"] }],
      }),
    );
    await this.#sourceClient.query(check_index_query).catch((err) => {
      console.error("ensureIndex error: %s", err);
      throw err;
    });

    // If it exists, check if it's active.
    const check_active_query = Select("active", Get(Index(indexName)));
    return await this.#sourceClient.query(check_active_query).catch((err) => {
      console.error("ensureIndex error: %s", err);
      throw err;
    });
  }

  async #ensureGetEventsFromCollection() {
    const qry = getEventsFromCollectionFunctionQuery(
      GET_EVENTS_FROM_COLLECTION,
    );

    const res = await this.#sourceClient.query(qry).catch((err) => {
      console.error("ensureGetEventsFromCollection error: %s", err);
      throw err;
    });

    return res;
  }

  async #ensureGetRemoveEventsFromCollection() {
    const qry = getRemoveEventsFromCollectionFunctionQuery(
      GET_REMOVE_EVENTS_FROM_COLLECTION,
    );

    const res = await this.#sourceClient.query(qry).catch((err) => {
      console.error("ensureGetRemoveEventsFromCollection error: %s", err);
      throw err;
    });

    return res;
  }

  async #validateTargetCollection(collectionName) {
    const qry = If(
      Exists(Collection(collectionName)),
      true,
      "Please make sure the collection exists on the destination DB",
    );

    const res = await this.#targetClient.query(qry).catch((err) => {
      console.error("validateTargetCollection error: %s", err);
      throw err;
    });

    return res;
  }

  async #getApplyEventQuery(e) {
    const docRef = e.doc;
    const docTs = e.ts;
    const docData = e.data;

    switch (e.action) {
      case "create":
        console.log(`Creating document: ${docRef} at ${docTs}`);
        return Let(
          {
            ref: docRef,
            doc: docData,
          },
          If(
            Exists(Var("ref")),
            "document already exists",
            Create(Var("ref"), { data: Var("doc") }),
          ),
        );

      case "update":
        if (docData == null) {
          // checking source data at ts
          const isNullData = await this.#sourceClient.query(
            Let(
              {
                ref: docRef,
                ts: docTs,
              },
              At(Var("ts"), IsNull(Select("data", Get(Var("ref"))))),
            ),
          );

          if (!isNullData) {
            console.log(`Skipping no-op update: ${docRef} at ${docTs}`);
            return null;
          }
        }
        console.log(`Updating document: ${docRef} at ${docTs}`);
        return Let(
          {
            ref: docRef,
            doc: docData,
          },
          If(
            Exists(Var("ref")),
            Update(Var("ref"), { data: Var("doc") }),
            "no such document",
          ),
        );

      case "remove":
      case "delete":
        console.log(`Deleting document: ${docRef} at ${docTs}`);
        return Let(
          {
            ref: docRef,
          },
          If(
            Exists(Var("ref")),
            Delete(Var("ref")),
            "Document does not exist or is already deleted",
          ),
        );

      default:
        throw `'${e.action}' is not a recognized Event action`;
    }
  }

  async #getRemoveEvents({ collectionName, startTime, duration }) {
    const events = [];

    let after = startTime;
    let before = null;

    while (!!after) {
      const qry = Call(GET_REMOVE_EVENTS_FROM_COLLECTION, [
        startTime,
        collectionName,
        duration,
        this.#defaultPageSize,
        after,
        before,
      ]);
      const res = await this.#sourceClient.query(qry).catch((err) => {
        console.error("getRemoveEvents error: %s", err);
        throw err;
      });

      events.push(...res.data);

      after = res.after ?? null;
    }

    return events;
  }

  async #getAllEvents({ indexName, startTime, duration }) {
    const events = [];

    let after = startTime;
    let before = null;

    while (!!after) {
      const qry = Call(GET_EVENTS_FROM_COLLECTION, [
        startTime,
        indexName,
        duration,
        this.#defaultPageSize,
        after,
        before,
      ]);

      const res = await this.#sourceClient
        .query(qry)
        .then((ret) => ret)
        .catch((err) => {
          console.error("getAllEvents error: %s", err);
          throw err;
        });

      const new_events = res.data.map((e) => e.data).flat();

      events.push(...new_events);

      after = res.after ?? null;
    }

    return events;
  }

  async #applyEvents(docEvents = [], collEvents = []) {
    const allEvents = docEvents.concat(collEvents);
    // combine updates and removes and sort them in timestamp order so they are replayed in the exact order
    const sortedEvents = allEvents.sort((e1, e2) => e1.ts - e2.ts);

    console.log(`Found ${sortedEvents.length} events`);

    while (sortedEvents.length > 0) {
      let seenIds = [];
      let eventQueries = [];

      for (
        let i = 0;
        i < this.#maxParallelism && sortedEvents.length > 0;
        i++
      ) {
        const evt = sortedEvents[0];

        if (!seenIds.includes(evt.doc.id)) {
          const eventQuery = await this.#getApplyEventQuery(evt);

          if (eventQuery) {
            eventQueries.push(eventQuery);
            seenIds.push(evt.doc.id);
          } else {
            i--;
          }

          sortedEvents.shift();
        } else {
          break;
        }
      }

      if (eventQueries.length > 0) {
        console.log(
          `Applying batch of ${eventQueries.length} event${
            eventQueries.length == 1 ? "" : "s"
          }`,
        );

        await this.#targetClient
          .query(Do(...eventQueries, null))
          .then((r) => r)
          .catch((err) => {
            console.error("Query error: %s", err);
            throw err;
          });
      }
    }
  }
}

module.exports = {
  MigrationClient,
};
