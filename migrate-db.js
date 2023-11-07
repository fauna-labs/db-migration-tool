const fauna = require("faunadb");
const {
  getEventsFromCollectionFunctionQuery,
  getRemoveEventsFromCollectionFunctionQuery
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

//Client for source DB
var sourceClient;

//Client for destination DB
var targetClient;

var get_events_from_collection = "_get_events_from_collection"
var get_remove_events_from_collection = "_get_remove_events_from_collection"

async function validateCollection(coll) {
  const qry = Let(
    {
      coll: coll,
    },
    If(
      Exists(Collection(Var("coll"))),
      Let(
        {
          h_days: Select("history_days", Get(Collection(Var("coll")))),
        },
        If(
          LT(0, Var("h_days")),
          Var("h_days"),
          "Please enable history for this collection"
        )
      ),
      "Please make sure the collection exists on the source DB"
    )
  );

  const res = await sourceClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => {
      console.error("validateCollection error: %s", err);
      throw err;
    });

  return res;
}

async function ensureIndex(index, collection) {
  const check_index_query = Let(
    {
      index: index,
      collection: collection,
    },
    If(Exists(Index(Var("index"))),
      true,
      // Doesn't exist, create it.
      CreateIndex({
        name: Var("index"),
        source: Collection(Var("collection")),
        values: [
          { field: ['ts'] },
          { field: ['ref'] }
        ]
      })
    )
  );

  // Returns true or an exception.
  await sourceClient
    .query(check_index_query)
    .then((ret) => ret)
    .catch((err) => {
      console.error("ensureIndex error: %s", err);
      throw err;
    });

  const check_active_query = Let(
    {
      index: index,
      collection: collection,
    },
    // If it exists, check if it's active.
    Let({
      is_active: Select('active', Get(Index(Var("index"))))
    },
      Var("is_active")
    )

  );

  return await sourceClient
    .query(check_active_query)
    .then((ret) => ret)
    .catch((err) => {
      console.error("ensureIndex error: %s", err);
      throw err;
    });
}

async function ensureGetEventsFromCollection() {
  const qry = getEventsFromCollectionFunctionQuery(get_events_from_collection)

  const res = await sourceClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => {
      console.error("ensureGetEventsFromCollection error: %s", err);
      throw err;
    });

  return res;
}

async function ensureGetRemoveEventsFromCollection() {
  const qry = getRemoveEventsFromCollectionFunctionQuery(get_remove_events_from_collection)

  const res = await sourceClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => {
      console.error("ensureGetRemoveEventsFromCollection error: %s", err);
      throw err;
    });

  return res;
}

async function validateTargetCollection(coll) {
  const qry = Let(
    {
      coll: coll,
    },
    If(
      Exists(Collection(Var("coll"))),
      true,
      "Please make sure the collection exists on the destination DB"
    )
  );

  const res = await targetClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => {
      console.error("validateTargetCollection error: %s", err);
      throw err;
    });

  return res;
}

async function getApplyEventQuery(e) {
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
          Create(Var("ref"), { data: Var("doc") })
        )
      );

    case "update":
      if (docData == null) {
        // checking source data at ts
        const isNullData = await sourceClient.query(
          Let(
            {
              ref: docRef,
              ts: docTs,
            },
            At(Var("ts"), IsNull(Select("data", Get(Var("ref")))))
          )
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
          "no such document"
        )
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
          "Document does not exist or is already deleted"
        )
      );

    default:
      throw `'${e.action}' is not a recognized Event action`;
  }
}

async function getRemoveEvents(coll, duration, size, removes) {
  var ts =
    lastProcessed.startTime > lastProcessed.removes.ts
      ? lastProcessed.startTime
      : lastProcessed.removes.ts;
  var after = lastProcessed.removes.after ?? null;
  var duration = duration;
  var before = null;
  var lastRef = lastProcessed.removes.ref;
  var lastTs = lastProcessed.removes.ts;
  const qry = Let(
    {
      ts: ts,
      coll: coll,
      duration: duration,
      size: size,
      before: before,
      after: after,
      get_remove_events_from_collection: get_remove_events_from_collection
    },
    Call(Var("get_remove_events_from_collection"), [
      Var("ts"),
      Var("coll"),
      Var("duration"),
      Var("size"),
      Var("after"),
      Var("before"),
    ])
  );
  const res = await sourceClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => {
      console.error("getRemoveEvents error: %s", err);
      throw err;
    });

  const len = res.data.length;

  if (len > 0) {
    removes.push(res.data);
    lastTs = res.data[len - 1].ts;
    lastRef = res.data[len - 1].doc;
  }

  if (res.after) {
    lastProcessed.removes.after = res.after;
    return await getRemoveEvents(coll, duration, size, removes);
  } else {
    delete lastProcessed.removes.after;
    lastProcessed.removes.ts = lastTs;
    lastProcessed.removes.ref = lastRef;
    return removes;
  }
}

async function getAllEvents(index, duration, size, liveEvents) {
  
  var startTime =
    lastProcessed.startTime > lastProcessed.updates.ts
      ? lastProcessed.startTime
      : lastProcessed.updates.ts;
  var after = lastProcessed.updates.after ?? null;
  var duration = duration;
  var before = null;
  var lastRef = lastProcessed.updates.ref;
  var lastTs = lastProcessed.updates.ts;

  const qry = Let(
    {
      startTime: startTime,
      index: index,
      duration: duration,
      size: size,
      before: before,
      after: after,
      get_events_from_collection: get_events_from_collection,
    },
    Call(Var("get_events_from_collection"), [
      Var("startTime"),
      Var("index"),
      Var("duration"),
      Var("size"),
      Var("after"),
      Var("before"),
    ])
  );
  const events = await sourceClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => {
      console.error("getAllEvents error: %s", err);
      throw err;
    });

  var length = events.data.length;

  if (length > 0) {
    liveEvents.push(events.data);
    lastRef = events.data.slice(-1)[0].data.slice(-1)[0].doc;
    lastTs = events.data.slice(-1)[0].data.slice(-1)[0].ts;
  }
  if (events.after) {
    lastProcessed.updates.after = events.after;
    return await getAllEvents(index, duration, size, liveEvents);
  } else {
    delete lastProcessed.updates.after;
    lastProcessed.updates.ref = lastRef;
    lastProcessed.updates.ts = lastTs;
    lastRef = null;
    lastTs = null;
    return liveEvents.map((ei) => ei.map((e) => e.data.map((ev) => ev)));
  }
}

async function flattenAndSortEvents(docEvents = [], collEvents = [], maxParallelism) {
  var allEvents = docEvents.concat(collEvents);
  const flattened = allEvents.flat(Infinity);
  // combine updates and removes and sort them in timestamp order so they are replayed in the exact order
  const sortedEvents = flattened.sort((e1, e2) => e1.ts - e2.ts);

  console.log(`Found ${sortedEvents.length} events`);

  while (sortedEvents.length > 0) {
    let seenIds = [];
    let eventQueries = [];

    for (let i = 0; i < maxParallelism && sortedEvents.length > 0; i++) {
      const evt = sortedEvents[0];

      if (!seenIds.includes(evt.doc.id)) {
        const eventQuery = await getApplyEventQuery(evt);

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
      console.log(`Applying batch of ${eventQueries.length} event${eventQueries.length == 1 ? "" : "s"}`);

      await targetClient
        .query(Do(...eventQueries, null))
        .then((r) => r)
        .catch((err) => {
          console.error("Query error: %s", err);
          throw err;
        });
    }
  }
}

async function migrate(coll, index, duration, size, sourceKey, targetKey, maxParallelism) {
  var liveEvents = [];
  var removes = [];

  sourceClient = new fauna.Client({
    secret: sourceKey,
  });

  targetClient = new fauna.Client({
    secret: targetKey,
  });

  //validate that the given collection exists and that history_days is set
  const valColl = await validateCollection(coll);
  if (typeof valColl != "number") {
    console.log(valColl);
    return false;
  }

  //verify that the index and functions exist, and if not then create them
  var indexActive = await ensureIndex(index, coll);
  //only continue if the index is active
  if (!indexActive) {
    console.log(`Index '${index}' is not active, retry later.`);
    return false;
  }

  await ensureGetEventsFromCollection();
  await ensureGetRemoveEventsFromCollection();

  //validate that the collection exists on destination db
  const valTargetColl = await validateTargetCollection(coll);
  if (typeof valTargetColl == "string") {
    console.log(valTargetColl);
    return false;
  }

  var docEvents = await getAllEvents(index, duration, size, liveEvents)
    .then((ev) => ev)
    .catch((e) => {
      throw e;
    });

  var collEvents = await getRemoveEvents(coll, duration, size, removes)
    .then((ev) => ev)
    .catch((e) => {
      throw e;
    });

  await flattenAndSortEvents(docEvents, collEvents, maxParallelism);

  return true;
}
//keep track of last fetched document ref and timestamp
var lastProcessed = {
  startTime: null,
  updates: { ref: null, ts: null },
  removes: { ref: null, ts: null },
};

/*var lastMigrated = {
  updates: { ref: null, ts: startTime },
  removes: { ref: null, ts: startTime },
};*/

module.exports = { lastProcessed, migrate };
