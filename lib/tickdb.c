#include "tickdb.h"

#define MIN_BLOCK_SIZE KIBIBYTES(64)

#include "tickdb_util.c"

bool string_equals(const void* k1, const void* k2, void* ctx) {
	string* s1 = (string*)k1;
	string* s2 = (string*)k2;
	if (string_data(s1) == NULL && string_data(s2) != NULL ||
			string_data(s1) != NULL && string_data(s2) == NULL)
		return false;
	bool res = string_cmp(s1, s2) == 0;

	return res;
}

u64 string_hash(const void* k1, size_t key_size, void* ctx) {
	string* s = (string*)k1;
	u64 res = wyhash(string_data(s), string_size(s));
	return res;
}

tdb_table* tdb_table_init(tdb_schema* s) {
  tdb_table* res = calloc(sizeof(tdb_table), 1);
	res->schema = s;
  res->largest_col = largest_col_size(s);
  res->blocks = hm_init(i32, tdb_block);
  res->symbol_uids = hm_init(string, i32);
	res->symbol_uids.equals = string_equals;
	res->symbol_uids.hasher = string_hash;

  return res;
}

void tdb_table_free(tdb_table* t) {
  close_columns(t);
  tdb_schema_free(t->schema);

	hm_iter(&t->blocks)
		vec_free((vec_tdb_block*)val);
  hm_free(&t->blocks);
  vec_free(&t->symbols);
  hm_free(&t->symbol_uids);
	free(t);
}

void tdb_table_write_data(tdb_table* t, void* data, size_t size) {
  tdb_col* cols = (tdb_col*)t->schema->columns.data;
  tdb_col* col = cols + t->col_index;
  if (col->data == NULL) {
    open_column(t, t->col_index);
  }
  memcpy(col->data + col->size, data, size);
  col->size += size;
  t->col_index = (t->col_index + 1) % t->schema->columns.len;
}

i32 tdb_table_stoi(tdb_table* t, char* symbol) {
  string s = string_init(symbol);
  i32* sym = _hm_get(&t->symbol_uids, &s);
  if (sym == NULL) {
    vec_push_ptr(&t->symbols, &s);
    sym = (i32*)_hm_put(&t->symbol_uids, &s, &t->symbols.len);
  }
	string_free(&s);
  return *sym;
}

char* tdb_table_itos(tdb_table* t, i64 symbol) {
  string* symbols = (string*)t->symbols.data;
  return sdata(symbols[symbol - 1]);
}

void tdb_table_write(tdb_table* t, char* symbol, i64 epoch_nanos) {
	i32 id = tdb_table_stoi(t, symbol);
  tdb_block* block = get_block(t, id, epoch_nanos);
  if (strlen(t->partition.name) == 0 || epoch_nanos < t->partition.ts_min ||
      epoch_nanos > t->partition.ts_max) {
    // Calling strftime for each row is bad perf, so instead compute min/max
    // ts's for partition
    struct tm time = nanos_to_tm(epoch_nanos);
    size_t written = strftime(t->partition.name, TDB_MAX_FMT_LEN,
                              sdata(t->schema->partition_fmt), &time);
    if (written == 0) {
      fprintf(stderr, "partition_fmt longer than %d\n", TDB_MAX_FMT_LEN);
      exit(EXIT_FAILURE);
    }

    t->partition.ts_min = min_partition_ts(t, epoch_nanos);
    t->partition.ts_max = max_partition_ts(t, epoch_nanos);
    printf("%s %lu %lu\n", t->partition.name, t->partition.ts_min,
           t->partition.ts_max);

    // Close existing open columns
    close_columns(t);
  }

  tdb_col* cols = (tdb_col*)t->schema->columns.data;
  size_t ts_stride = column_stride(t->schema, cols->type);
  tdb_table_write_data(t, &epoch_nanos, ts_stride);
}
