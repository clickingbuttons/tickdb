#include "tickdb.h"

#define MIN_BLOCK_SIZE KIBIBYTES(64)

#include "tickdb_util.c"

tdb_table tdb_table_init(tdb_schema* s) {
  tdb_schema schema_copy;
  memcpy(&schema_copy, s, sizeof(tdb_schema));
  size_t sym_size = column_stride(s, s->sym_type);

  tdb_table res = {
   .schema = schema_copy,
   .largest_col = get_largest_col_size(s),
   .partition = {0},
   .blocks = hm_init(sym_size, tdb_block),
   .symbols = vec_init(string),
   .symbol_uids = _hm_init(sizeof(string), sym_size),
  };

  return res;
}

void tdb_table_close(tdb_table* t) {
  close_columns(t);
	tdb_schema_free(&t->schema);

  hm_free(&t->blocks);
  vec_free(&t->symbols);
  hm_free(&t->symbol_uids);
}

void tdb_table_write_data(tdb_table* t, void* data, size_t size) {
  tdb_col* cols = (tdb_col*)t->schema.columns.data;
  tdb_col* col = cols + t->col_index;
  if (col->data == NULL) {
    open_column(t, t->col_index);
  }
  memcpy(col->data + col->size, data, size);
  col->size += size;
  t->col_index = (t->col_index + 1) % t->schema.columns.size;
}

size_t tdb_table_stoi(tdb_table* t, char* symbol) {
  string s = string_init(symbol);
  size_t* sym = _hm_get(&t->symbol_uids, &s);
  if (sym == NULL) {
    _vec_push(&t->symbols, &s);
    sym = (size_t*)_hm_put(&t->symbol_uids, &s, &t->symbols.size);
  }
  return *sym;
}

char* tdb_table_itos(tdb_table* t, i64 symbol) {
  string* symbols = (string*)t->symbols.data;
  return sdata(symbols[symbol - 1]);
}

void tdb_table_write(tdb_table* t, char* symbol, i64 epoch_nanos) {
  tdb_block* block = get_block(t, tdb_table_stoi(t, symbol), epoch_nanos);
  if (strlen(t->partition.name) == 0 || epoch_nanos < t->partition.ts_min ||
      epoch_nanos > t->partition.ts_max) {
    // Calling strftime for each row is bad perf, so instead compute min/max
    // ts's for partition
    struct tm time = nanos_to_tm(epoch_nanos);
    size_t written = strftime(t->partition.name, TDB_MAX_PARTITIONFMT_LEN,
                              sdata(t->schema.partition_fmt), &time);
    if (written == 0) {
      fprintf(stderr, "partition_fmt longer than %d\n",
              TDB_MAX_PARTITIONFMT_LEN);
      exit(EXIT_FAILURE);
    }

    t->partition.ts_min = min_partition_ts(t, epoch_nanos);
    t->partition.ts_max = max_partition_ts(t, epoch_nanos);
    printf("%s %lu %lu\n", t->partition.name, t->partition.ts_min,
           t->partition.ts_max);

    // Close existing open columns
    close_columns(t);
  }

	tdb_col* cols = (tdb_col*) t->schema.columns.data;
	size_t ts_stride = column_stride(&t->schema, cols->type);
  tdb_table_write_data(t, &epoch_nanos, ts_stride);
}
