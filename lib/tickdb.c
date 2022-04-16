#include "column.h"
#include "tickdb.h"

#define MIN_BLOCK_SIZE KIBIBYTES(64)

static u64 hm_string_hash(const void* key, size_t _, void* __) {
	return string_hash((string*)key);
}

static bool hm_string_equals(const void* key1, const void* key2, void* _) {
	string* s1 = (string*)key1;
	string* s2 = (string*)key2;
	return string_equals((string*)s1, (string*)s2);
}

tdb_table* tdb_table_init(tdb_schema* s) {
	tdb_table* res = calloc(sizeof(tdb_table), 1);
	res->schema = s;
	res->largest_col = max_col_stride(s);
	res->blocks = hm_init(i32, vec_tdb_block);
	res->symbol_uids = hm_init(string, i32);
	res->symbol_uids.equals = hm_string_equals;
	res->symbol_uids.hasher = hm_string_hash;

	return res;
}

i32 tdb_table_close(tdb_table* t) {
	for_each(col, t->schema->columns)
		if (col_close(col))
			return 1;
	tdb_schema_free(t->schema);

	hm_iter(&t->blocks) vec_free((vec_tdb_block*)val);
	hm_free(&t->blocks);
	vec_free(&t->symbols);
	hm_free(&t->symbol_uids);
	free(t);

	return 0;
}

void tdb_write_sym(tdb_table* t, char* symbol) {
}

i32 tdb_table_stoi(tdb_table* t, char* symbol) {
	string s = string_init(symbol);
	i32* sym = _hm_get(&t->symbol_uids, &s);
	if (sym == NULL) {
		vec_push_ptr(&t->symbols, &s);
		tdb_write_sym(t, symbol);
		sym = (i32*)_hm_put(&t->symbol_uids, &s, &t->symbols.len);
	}
	string_free(&s);
	return *sym;
}

char* tdb_table_itos(tdb_table* t, i64 symbol) {
	string* symbols = (string*)t->symbols.data;
	return sdata(symbols[symbol - 1]);
}

static tdb_block* get_block(tdb_table* t, i32 symbol, i64 nanos) {
	vec_tdb_block* blocks = _hm_get(&t->blocks, &symbol);
	if (blocks == NULL) {
		vec_tdb_block new_blocks = {0};
		blocks = hm_put(t->blocks, symbol, new_blocks);
	}

	for_each(b, *blocks) if (nanos >= b->ts_min) return b;

	tdb_block new_block = {
	 .symbol = symbol,
	 .ts_min = nanos,
	};
	vec_push_ptr(blocks, &new_block);

	return blocks->data + blocks->len;
}

i32 tdb_table_write(tdb_table* t, char* symbol, i64 epoch_nanos) {
	i32 id = tdb_table_stoi(t, symbol);
	tdb_block* block = get_block(t, id, epoch_nanos);
	if (strlen(t->partition.name) == 0 || epoch_nanos < t->partition.ts_min ||
		epoch_nanos > t->partition.ts_max) {
		struct tm time = nanos_to_tm(epoch_nanos);
		size_t written = strftime(t->partition.name, TDB_MAX_FMT_LEN,
								  sdata(t->schema->partition_fmt), &time);
		if (written == 0) {
			fprintf(stderr, "partition_fmt longer than %d\n", TDB_MAX_FMT_LEN);
			return 1;
		}

		// Calling strftime for each row is bad perf, so instead compute min/max
		// ts's for partition
		t->partition.ts_min = min_partition_ts(&t->schema->partition_fmt, epoch_nanos);
		t->partition.ts_max = max_partition_ts(&t->schema->partition_fmt, epoch_nanos);
		printf("new partition %s min %lu max %lu\n", t->partition.name, t->partition.ts_min,
			   t->partition.ts_max);

		// Open new columns
		for_each(col, t->schema->columns) {
			if (col_close(col))
				return 2;
			if (col_open(col, &t->schema->name, t->partition.name))
				return 3;
		}
	}

	tdb_col* ts_col = t->schema->columns.data;
	tdb_table_write_data(t, &epoch_nanos, ts_col->stride);

	return 0;
}

i32 tdb_table_write_data(tdb_table* t, void* data, size_t size) {
	tdb_col* col = (tdb_col*)t->schema->columns.data + t->col_index;
	if (col->len + 1 > col->capacity)
		col_grow(col, col->capacity * 2);
	memcpy(col->data + col->len * col->stride, data, size);
	col->len += 1;
	t->col_index = (t->col_index + 1) % t->schema->columns.len;

	return 0;
}
