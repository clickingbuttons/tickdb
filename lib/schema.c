#include "schema.h"

tdb_schema tdb_schema_init(char* name, char* ts_partition_fmt,
                           tdb_coltype sym_type, char* sym_universe) {
  tdb_schema res = {
   .name = string_init(name),
   .sym_name = string_init("sym"),
   .ts_name = string_init("ts"),
   .ts_stride = 8, // TODO: optimize lower resolutions
   .partition_fmt = string_init(ts_partition_fmt),
   .sym_universe = string_init(sym_universe),
   .columns = vec_init(tdb_col),
   .sym_type = sym_type,
  };

  tdb_col ts = {
   .name = string_init("ts"),
   .type = TDB_TIMESTAMP,
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
  tdb_col* cols = (tdb_col*)s->columns.data;
  for (int i = 0; i < s->columns.size; i++) {
    tdb_col* col = cols + i;
    string_free(&col->name);
  }
}
