import fauna from "faunadb";

const { Let, Var, ToMicros, Time, Call, Exists, If, Create, Update, Delete } =
  fauna.query;

//Client for source DB
var classicClient = new fauna.Client({
  secret: "secret",
});
//Client for Destination DB
var usClient = new fauna.Client({
  secret: "secret",
});

async function applyEvents(e) {
  const docRef = e.doc;
  const docTs = e.ts;
  const docData = e.data;
  switch (e.action) {
    case "create":
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
      await usClient
        .query(createQuery)
        .then((r) => r)
        .catch((err) => console.error("Error: %s", err));
      break;
    case "update":
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
      await usClient
        .query(updateQuery)
        .then((r) => r)
        .catch((err) => console.error("Error: %s", err));
      break;
    case "delete":
      console.log(`in delete ref: ${e.doc}`);

      break;
    default:
      console.log("Something isn't right");
  }
}

async function applyDeletes(ref) {
  const deleteQuery = Let(
    {
      ref: ref,
    },
    If(Exists(Var("ref")), Delete(Var("ref")), "already deleted")
  );

  await usClient
    .query(deleteQuery)
    .then((r) => console.log(r))
    .catch((err) => console.error("Error: %s", err));
}

export async function getRemoveEvents(ts, coll, size, before, after) {
  var lastProcessedRef = null;
  var lastProcessedTS = null;
  const qry = Let(
    {
      ts: ToMicros(Time(ts)),
      coll: coll,
      size: size,
      before: before,
      after: after,
    },
    Call("get_remove_events_from_collection", [
      Var("ts"),
      Var("coll"),
      Var("size"),
      Var("after"),
      Var("before"),
    ])
  );
  const res = await classicClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => console.error("Error: %s", err));

  const len = res.data.length;
  if (len > 0) {
    lastProcessedRef = res.data[len - 1].ref;
    lastProcessedTS = res.data[len - 1].ts;
  }

  res.data.map((r) => applyDeletes(r.ref));

  if (res.after) {
    if (
      res.after.document != lastProcessedRef &&
      res.after.ts != lastProcessedTS
    )
      return getRemoveEvents(ts, coll, size, before, res.after);
  }

  return [lastProcessedRef, lastProcessedTS];
}

export async function getAllEvents(
  startTime,
  endTime,
  index,
  size,
  before,
  after
) {
  var lastProcessedRef = null;
  var lastProcessedTS = null;
  const qry = Let(
    {
      startTime: ToMicros(Time(startTime)),
      endTime: endTime,
      index: index,
      size: size,
      before: before,
      after: after,
    },
    Call("get_events_from_collection", [
      Var("startTime"),
      Var("endTime"),
      Var("index"),
      Var("size"),
      Var("after"),
      Var("before"),
    ])
  );
  const events = await classicClient
    .query(qry)
    .then((ret) => ret)
    .catch((err) => console.error("Error: %s", err));

  const len = events.data.length;
  if (len > 0) {
    lastProcessedRef = events.data.slice(-1)[0].data.slice(-1)[0].doc;
    lastProcessedTS = events.data.slice(-1)[0].data.slice(-1)[0].ts;
  }

  events.data.map((ei) => ei.data.map((e) => applyEvents(e)));

  if (events.after) {
    if (
      events.after[1] != lastProcessedRef &&
      events.after[0] != lastProcessedTS
    )
      return getAllEvents(
        startTime,
        endTime,
        index,
        size,
        before,
        events.after
      );
  }

  return [lastProcessedRef, lastProcessedTS];
}

const run = async () => {
  const targetTime = "2023-07-01T00:00:00Z";
  const coll = "Genre"; //collection name
  const index = "Genre_Events"; //index name
  const size = 5; //page size
  const after = null;
  const before = null;

  const afterRemove = await getRemoveEvents(
    targetTime,
    coll,
    size,
    before,
    after
  )
    .then((res) => res)
    .catch((e) => console.log(e));
  const endTime = afterRemove[1];
  //console.log(endTime);
  await getAllEvents(targetTime, endTime, index, size, before, after)
    .then((ev) => ev)
    .catch((e) => console.log(e));
};
//

run();
