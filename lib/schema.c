#include "schema.h"

tdb_schema tdb_schema_init(char* name, char* partition_fmt,
                           tdb_coltype sym_type, char* sym_universe) {
  tdb_schema res = {
   .name = string_init(name),
   .ts_name = string_init("ts"),
   .partition_fmt = string_init(partition_fmt),
   .sym_name = string_init("sym"),
   .sym_type = sym_type,
   .sym_universe = string_init(sym_universe),
  };

  // TODO: support "resolution" which downscales "epoch_nanos"
	// >>> math.log2(24*60) Minutes
  // 10.491853096329674
  // >>> math.log2(24*60*60) Seconds
  // 16.398743691938193
  // >>> math.log2(24*60*60*10000) .1ms
  // 29.686456071487644
  tdb_col ts = {
   .name = string_init("ts"),
   .type = TDB_TIMESTAMP64,
  };
  vec_push(&res.columns, ts);

  return res;
}

void tdb_schema_add(tdb_schema* s, tdb_coltype type, char* name) {
  tdb_col col = {
   .name = string_init(name),
   .type = type,
  };

  vec_push(&s->columns, col);
}

void tdb_schema_free(tdb_schema* s) {
  string_free(&s->sym_name);
  string_free(&s->ts_name);
  for (int i = 0; i < s->columns.len; i++) {
    tdb_col* col = s->columns.data + i;
    string_free(&col->name);
  }
}
