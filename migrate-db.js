const fauna = require("faunadb");

const {
  Let,
  Get,
  Var,
  Index,
  ToMicros,
  Time,
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
} = fauna.query;

//Client for source DB
var sourceClient;

//Client for destination DB
var targetClient;

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
    .catch((err) => console.error("Error: %s", err));

  return res;
}

async function ensureIndex(index, collection) {
  const qry = Let(
    {
      index: index,
      collection: collection,
    },
    If(Exists(Index(Var("index"))),
      true,
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

  const res = await sourceClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => console.error("Error: %s", err));

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
    .catch((err) => console.error("Error: %s", err));

  return res;
}

async function applyEvents(e) {
  const docRef = e.doc;
  const docTs = e.ts;
  const docData = e.data;
 
  switch (e.action) {
    case "create":
      console.log("Creating document ", docRef)
      const createQuery = Let(
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
      await targetClient
        .query(createQuery)
        .then((r) => r)
        .catch((err) => console.error("Error: %s", err));
      //lastMigrated.updates.ref = docRef;
      //lastMigrated.updates.ts = docTs;
      break;

    case "update":
      console.log("Updating document ", docRef);
      const updateQuery = Let(
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
      await targetClient
        .query(updateQuery)
        .then((r) => r)
        .catch((err) => console.error("Error: %s", err));
      //lastMigrated.updates.ref = docRef;
      //lastMigrated.updates.ts = docTs;
      break;

    case "remove":
    case "delete":
      console.log("Deleting document ", docRef)
      const removeQuery = Let(
        {
          ref: docRef,
        },
        If(
          Exists(Var("ref")),
          Delete(Var("ref")),
          "Document does not exist or is already deleted"
        )
      );
      await targetClient
        .query(removeQuery)
        .then((r) => r)
        .catch((err) => console.error("Error: %s", err));
      //lastMigrated.removes.ref = docRef;
      //lastMigrated.removes.ts = docTs;
      break;

    default:
      console.log("Something isn't right, check your inputs");
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
    },
    Call("get_remove_events_from_collection", [
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
    .catch((err) => console.error("Error: %s", err));

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
    },
    Call("get_events_from_collection", [
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
    .catch((err) => console.error("Error: %s", err));

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

async function flattenAndSortEvents(docEvents = [], collEvents = []) {
  var allEvents = docEvents.concat(collEvents);
  const flattened = allEvents.flat(Infinity);
  // combine updates and removes and sort them in timestamp order so they are replayed in the exact order
  const sortedEvents = flattened.sort((e1, e2) => e1.ts - e2.ts);

  console.log(`Found ${sortedEvents.length} events`);

  sortedEvents.map(async (e) => await applyEvents(e));
}

async function migrate(coll, index, duration, size, sourceKey, targetKey) {
  var liveEvents = [];
  var removes = [];

  console.log( sourceKey);
  sourceClient = new fauna.Client({
    secret: 'fnAFRoDfmkACRFXHlhx5xj_wUDratlx-89Fao5BO',
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

  //verify that the index exists, and if not then create it
  const valIndex = await ensureIndex(index, coll);

  //validate that the collection exists on destination db
  const valTargetColl = await validateTargetCollection(coll);
  if (typeof valTargetColl == "string") {
    console.log(valTargetColl);
    return false;
  }

  var docEvents = await getAllEvents(index, duration, size, liveEvents)
    .then((ev) => ev)
    .catch((e) => console.log(e));

  var collEvents = await getRemoveEvents(coll, duration, size, removes)
    .then((ev) => ev)
    .catch((e) => console.log(e));

  await flattenAndSortEvents(docEvents, collEvents);

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
