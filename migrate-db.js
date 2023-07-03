import fauna from "faunadb";

const {
  Delete,
  Let,
  Var,
  ToMicros,
  Time,
  Paginate,
  Collections,
  Call,
  Exists,
  If,
  Create,
  Update,
} = fauna.query;

// Fauna client in Classic RG
var classicClient = new fauna.Client({
  secret: "secret",
});

//Fauna client in US RG
var usClient = new fauna.Client({
  secret: "secret",
});


const getEventsFromClassic = (ts) =>
  Let(
    {
      ts: ts,
      targetTime: ToMicros(Time(Var("ts"))),
    },
    Call("get_all_events", Var("targetTime"))
  );

const getDeletedEvents = Call("get_remove_events");

async function applyEvents(e) {
  const docRef = e.doc;
  const docData = e.data;
  switch (e.action) {
    case "create":
      console.log(`in create ref: ${e.doc}`);
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
      console.log(`in update ref: ${e.doc}`);
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
      console.log("Something isn't right. Check your inputs");
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

const run = async () => {
  //capture and playback create and update events
  const events = await classicClient
    .query(getEventsFromClassic("2023-07-01T06:06:42Z"))
    .then((ret) => ret)
    .catch((err) => console.error("Error: %s", err));

  events.data.map((ei) =>
    ei.data.map((e) => e.data.map((ev) => applyEvents(ev)))
  );

  const dels = await classicClient
    .query(getDeletedEvents)
    .then((ret) => ret.data)
    .catch((err) => console.error("Error: %s", err));
 //capture and playback remove events
  dels.map((del) => del.data.map((d) => applyDeletes(d)));
};

run();
