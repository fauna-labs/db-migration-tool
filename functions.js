// @ts-check

const fauna = require("faunadb");

const {
  Let,
  Var,
  Index,
  ToMicros,
  If,
  Select,
  LT,
  Match,
  Query,
  Lambda,
  Equals,
  TimeAdd,
  Epoch,
  Paginate,
  Range,
  Map,
  Events,
  Filter,
  And,
  GTE,
  IsNonEmpty,
  Function,
  CreateFunction,
  Exists,
  Documents,
  Collection,
  Update,
} = fauna.query;

function getEventsFromCollectionFunctionQuery(name) {
  return Let(
    {
      body: Query(
        Lambda(
          ["startTime", "indexName", "interval", "size", "after", "before"],
          Let(
            {
              match: Match(Index(Var("indexName"))),
              endTime: ToMicros(
                TimeAdd(
                  Epoch(Var("startTime"), "microseconds"),
                  Var("interval"),
                  "minutes",
                ),
              ),
              page: If(
                Equals(Var("before"), null),
                If(
                  Equals(Var("after"), null),
                  Paginate(Range(Var("match"), Var("startTime"), []), {
                    size: Var("size"),
                  }),
                  Paginate(Range(Var("match"), Var("startTime"), []), {
                    after: Var("after"),
                    size: Var("size"),
                  }),
                ),
                Paginate(Range(Var("match"), Var("startTime"), []), {
                  before: Var("before"),
                  size: Var("size"),
                }),
              ),
              refs: Map(Var("page"), Lambda(["t", "r"], Var("r"))),
              eventList: Map(
                Var("refs"),
                Lambda("e", Paginate(Events(Var("e")), { size: 100000 })),
              ),
              recent: Map(
                Var("eventList"),
                Lambda(
                  "el",
                  Filter(
                    Var("el"),
                    Lambda(
                      "t",
                      And(
                        GTE(Select("ts", Var("t")), Var("startTime")),
                        LT(Select("ts", Var("t")), Var("endTime")),
                      ),
                    ),
                  ),
                ),
              ),
              filtered: Filter(
                Var("recent"),
                Lambda("el", IsNonEmpty(Var("el"))),
              ),
            },
            Map(
              Var("filtered"),
              Lambda(
                "res",
                Map(
                  Var("res"),
                  Lambda("event", {
                    ts: Select("ts", Var("event")),
                    doc: Select("document", Var("event")),
                    action: Select("action", Var("event")),
                    data: Select("data", Var("event"), null),
                  }),
                ),
              ),
            ),
          ),
        ),
      ),
    },
    If(
      Exists(Function(name)),
      Update(Function(name), { body: Var("body") }),
      CreateFunction({
        name,
        body: Var("body"),
      }),
    ),
  );
}

function getRemoveEventsFromCollectionFunctionQuery(name) {
  return Let(
    {
      body: Query(
        Lambda(
          ["startTime", "collection", "interval", "size", "after", "before"],
          Let(
            {
              match: Documents(Collection(Var("collection"))),
              endTime: ToMicros(
                TimeAdd(
                  Epoch(Var("startTime"), "microseconds"),
                  Var("interval"),
                  "minutes",
                ),
              ),
              page: If(
                Equals(Var("before"), null),
                If(
                  Equals(Var("after"), null),
                  Paginate(Var("match"), {
                    after: { ts: Var("startTime") },
                    size: Var("size"),
                    events: true,
                  }),
                  Paginate(Var("match"), {
                    after: Var("after"),
                    size: Var("size"),
                    events: true,
                  }),
                ),
                Paginate(Var("match"), {
                  before: Var("before"),
                  size: Var("size"),
                  events: true,
                }),
              ),
              removeEvents: Filter(
                Var("page"),
                Lambda(
                  "e",
                  And(
                    Equals(Select(["action"], Var("e")), "remove"),
                    GTE(Select("ts", Var("e")), Var("startTime")),
                    LT(Select("ts", Var("e")), Var("endTime")),
                  ),
                ),
              ),
            },
            Map(
              Var("removeEvents"),
              Lambda("e", {
                ts: Select("ts", Var("e")),
                doc: Select("document", Var("e")),
                action: Select("action", Var("e")),
              }),
            ),
          ),
        ),
      ),
    },
    If(
      Exists(Function(name)),
      Update(Function(name), { body: Var("body") }),
      CreateFunction({
        name,
        body: Var("body"),
      }),
    ),
  );
}

module.exports = {
  getEventsFromCollectionFunctionQuery,
  getRemoveEventsFromCollectionFunctionQuery,
};
