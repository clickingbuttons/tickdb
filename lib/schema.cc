#include "schema.h"

tdb_schema* tdb_schema_init(char* name, char* partition_fmt,
                           tdb_coltype sym_type, char* sym_universe) {
  tdb_schema* res = new tdb_schema();
  res->name = string_init(name);
  res->ts_name = string_init("ts");
  res->partition_fmt = string_init(partition_fmt);
  res->sym_name = string_init("sym");
  res->sym_type = sym_type;
  res->sym_universe = string_init(sym_universe);

  // TODO: support "resolution" which downscales "epoch_nanos"
	// >>> math.log2(24*60) Minutes
  // 10.491853096329674
  // >>> math.log2(24*60*60) Seconds
  // 16.398743691938193
  // >>> math.log2(24*60*60*10000) .1ms
  // 29.686456071487644
  res->columns.push_back({
   .name = string_init("ts"),
   .type = TDB_TIMESTAMP64,
  });

  return res;
}

void tdb_schema_add(tdb_schema* s, tdb_coltype type, char* name) {
  s->columns.push_back({
   .name = string_init(name),
   .type = type,
  });
}

void tdb_schema_free(tdb_schema* s) {
	s->~tdb_schema();
  free(s);
}

size_t tdb_schema::column_stride(tdb_coltype type) {
  switch (type) {
  case TDB_SYMBOL8:
  case TDB_INT8:
  case TDB_UINT8:
  case TDB_TIMESTAMP8:
    return 1;
  case TDB_SYMBOL16:
  case TDB_INT16:
  case TDB_UINT16:
  case TDB_TIMESTAMP16:
    return 2;
  case TDB_SYMBOL32:
  case TDB_INT32:
  case TDB_UINT32:
  case TDB_FLOAT:
  case TDB_TIMESTAMP32:
    return 4;
  case TDB_SYMBOL64:
  case TDB_CURRENCY:
  case TDB_INT64:
  case TDB_UINT64:
  case TDB_DOUBLE:
  case TDB_TIMESTAMP64:
    return 8;
  case TDB_TIMESTAMP:
    fprintf(stderr,
            "cannot know stride of TDB_TIMESTAMP. must specify TDB_TIMESTAMP64, "
            "TDB_TIMESTAMP32, TDB_TIMESTAMP16, or TDB_TIMESTAMP8\n");
    exit(1);
  }
}
