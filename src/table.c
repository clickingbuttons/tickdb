#include "table.h"
#include "util/time.h"

#include <limits.h>

#define COL_DEFAULT_CAP 10000000
// 8-byte column: 512k
// 4-byte column: 256k
// 2-byte column: 128k
// 1-byte column: 64k
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

	res->partition.name = string_empty;
	string_printf(&res->data_path, "data/%p", &s->name);

	string_printf(&res->symbol_path, "%p/%p.%s", &res->data_path,
				  &s->sym_universe, column_ext(s->sym_type));
	if (mkdirp(sdata(res->symbol_path)))
		return NULL;

	res->symbol_file = fopen(sdata(res->symbol_path), "a");
	if (res->symbol_file == NULL) {
		TDB_ERRF_SYS("open symbol file %s", sdata(res->symbol_path));
		return NULL;
	}
	if (hm_init(&res->symbol_uids, sizeof(string), sizeof(i32), NULL))
    return NULL;
	res->symbol_uids.equals = hm_string_equals;
	res->symbol_uids.hasher = hm_string_hash;

	return res;
}

i32 tdb_table_close(tdb_table* t) {
	tdb_schema_free(t->schema);
	string_free(&t->data_path);

	string_free(&t->partition.name);
	hm_iter(&t->blocks) vec_free((vec_tdb_block*)val);
	hm_free(&t->blocks);
	string_free(&t->symbol_path);
	fclose(t->symbol_file);
	vec_free(&t->symbols);
	hm_free(&t->symbol_uids);
	free(t);

	return 0;
}

static void tdb_write_sym(tdb_table* t, string* symbol, size_t sym_num) {
	if (sym_num != 0)
		fwrite("\n", 1, 1, t->symbol_file);
	fwrite(string_data(symbol), string_len(symbol), 1, t->symbol_file);
}

i32 tdb_table_stoi(tdb_table* t, const char* symbol) {
	string s = string_init(symbol);
	i32* sym = _hm_get(&t->symbol_uids, &s);
	if (sym == NULL) {
		tdb_write_sym(t, &s, t->symbols.len);
		vec_push_ptr(&t->symbols, &s);
		sym = (i32*)_hm_put(&t->symbol_uids, &s, &t->symbols.len);
	}
	string_free(&s);
	return *sym;
}

char* tdb_table_itos(tdb_table* t, i64 symbol) {
	return sdata(t->symbols.data[symbol - 1]);
}

static tdb_block* get_block(tdb_table* t, i32 symbol, i64 nanos) {
	vec_tdb_block* blocks = _hm_get(&t->blocks, &symbol);
	if (blocks == NULL) {
		vec_tdb_block new_blocks = {0};
		blocks = hm_put(t->blocks, symbol, new_blocks);
	}

	for_each(b, *blocks) {
		if (nanos >= b->ts_min && b->len < MIN_BLOCK_SIZE) {
			b->len += 1;
			return b;
		}
	}

	tdb_block new_block = {
	 .symbol = symbol,
	 .ts_min = nanos,
	 .num = t->partition.num_blocks++,
	};
	vec_push_ptr(blocks, &new_block);

	return blocks->data + blocks->len - 1;
}

static bool is_old_partition(tdb_partition* p, i64 epoch_nanos) {
	if (string_len(&p->name) == 0)
		return true;
	if (epoch_nanos < p->ts_min || epoch_nanos > p->ts_max)
		return true;

	return false;
}

i32 tdb_table_write(tdb_table* t, const char* symbol, i64 epoch_nanos) {
	i32 id = tdb_table_stoi(t, symbol);
	tdb_schema* s = t->schema;
	tdb_partition* p = &t->partition;
	if (is_old_partition(p, epoch_nanos)) {
		// New partition
		struct tm time = nanos_to_tm(epoch_nanos);
		string_grow(&p->name, PATH_MAX);
		p->name._size =
		 strftime(sdata(p->name), PATH_MAX, sdata(s->partition_fmt), &time);
		printf("partition %s\n", sdata(p->name));

		p->ts_min = min_partition_ts(&s->partition_fmt, epoch_nanos);
		p->ts_max = max_partition_ts(&s->partition_fmt, epoch_nanos);

		// Open new block index
    string path;
    string_printf(&path, "%p/%p/blocks.unsorted", &t->data_path, &p->name);
    hm_init(&t->blocks, sizeof(i32), sizeof(vec_tdb_block), sdata(path)); \
    string_free(&path);

		// Open new columns
		for_each(col, s->columns) {
			if (mmaped_file_close(&col->file))
				return 2;
      string path = string_empty;
			string_printf(&path, "%p/%p/%p.%s", &t->data_path,
						  &p->name, &col->name, column_ext(col->type));

			if (mmaped_file_open(&col->file, sdata(path)))
				return 3;
      if (mmaped_file_resize(&col->file, COL_DEFAULT_CAP * col->stride))
        return 4;
      string_free(&path);
		}
	}

	t->block = get_block(t, id, epoch_nanos);
	tdb_col* ts_col = s->columns.data;
	tdb_table_write_data(t, &epoch_nanos, ts_col->stride);

	return 0;
}

static inline char* get_dest(tdb_block* block, tdb_col* col) {
	off_t offset = (block->num * col->block_size) + (block->len * col->stride);
	char* res = col->file.data + offset;
	// printf("n %lu l %d p %p off %lu res %p\n", block->num, block->len,
	// col->data, offset, res);

	return res;
}

i32 tdb_table_write_data(tdb_table* t, void* data, i64 size) {
	tdb_col* col = (tdb_col*)t->schema->columns.data + t->col_index;

	char* dest = get_dest(t->block, col);
	if (dest + col->stride > col->file.data + col->file.size) {
    if (mmaped_file_resize(&col->file, col->file.size * 2))
      return 1;
		dest = get_dest(t->block, col);
	}
	memcpy(dest, data, size);

	t->col_index = (t->col_index + 1) % t->schema->columns.len;
	return 0;
}
