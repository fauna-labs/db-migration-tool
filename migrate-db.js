import fauna from "faunadb";

const { Let, Var, ToMicros, Time, Call, Exists, If, Create, Update, Delete } =
  fauna.query;

//Client for source DB
var classicClient = new fauna.Client({
  secret: "secret-src",
});
//Client for Destination DB
var usClient = new fauna.Client({
  secret: "secret-dest",
});

async function applyEvents(e) {
  const docRef = e.doc;
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
        .then((r) => console.log(r))
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
        .then((r) => console.log(r))
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

async function getRemoveEvents(ts, coll, size, before, after) {
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

  res.data.map((r) => applyDeletes(r));

  if (res.after) {
    getRemoveEvents(ts, coll, size, before, res.after);
  }
}

async function getAllEvents(ts, index, size, before, after) {
  const qry = Let(
    {
      ts: ToMicros(Time(ts)),
      index: index,
      size: size,
      before: before,
      after: after,
    },
    Call("get_events_from_collection", [
      Var("ts"),
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

  events.data.map((ei) => ei.data.map((e) => applyEvents(e)));

  if (events.after) {
    getAllEvents(ts, index, size, before, events.after);
  }
}

const targetTime = "2023-07-09T00:00:00Z";
const coll = "Book"; //collection name
const index = "book_modified_docs"; //index name
const size = 100; //page size
const after = null;
const before = null;

getAllEvents(targetTime, index, size, after, before);

getRemoveEvents(targetTime, coll, size, after, before);
