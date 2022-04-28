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

static string get_data_path(const char* name) {
	string res = string_empty;
	string_printf(&res, "data/%s", name);

	return res;
}

static string get_schema_path(const char* data_path) {
	string res = string_empty;
	string_printf(&res, "%s/_schema", data_path);

	return res;
}

tdb_table* tdb_table_init(tdb_schema* s) {
	tdb_table* res = calloc(sizeof(tdb_table), 1);
	res->schema = s;
	res->partition.name = string_empty;
	res->data_path = get_data_path(sdata(s->name));
	res->schema_path = get_schema_path(sdata(res->data_path));
	res->symbol_path = string_empty;
	string_printf(&res->symbol_path, "%p/%p.%s", &res->data_path,
				  &s->sym_universe, column_ext(s->sym_type));
	if (mkdirp(sdata(res->schema_path)))
		return NULL;

	FILE* schema_file = fopen(sdata(res->schema_path), "w");
	if (schema_file == NULL) {
		TDB_ERRF_SYS("open schema file %s", sdata(res->schema_path));
		return NULL;
	}
	schema_serialize(s, schema_file);
	if (fclose(schema_file)) {
		TDB_ERRF_SYS("close schema file %s", sdata(res->schema_path));
		return NULL;
	}

	res->symbol_file = fopen(sdata(res->symbol_path), "a");
	if (res->symbol_file == NULL) {
		TDB_ERRF_SYS("open symbol file %s", sdata(res->symbol_path));
		return NULL;
	}
	if (hm_init(&res->symbol_uids, sizeof(string), sizeof(i32), NULL))
		return NULL;
	res->symbol_uids.equals = hm_string_equals;
	res->symbol_uids.hasher = hm_string_hash;
	res->block_pool.used = 0;

	return res;
}

tdb_table* tdb_table_open(const char* name) {
	string data_path = get_data_path(name);
	string schema_path = get_schema_path(sdata(data_path));

	FILE* f = fopen(sdata(schema_path), "r");
	if (f == NULL) {
		TDB_ERRF_SYS("open schema file %s", sdata(schema_path));
		return NULL;
	}

	tdb_schema* s = schema_deserialize(f, name);

	return tdb_table_init(s);
}

static int cmp_blocks(const void* a, const void* b) {
	tdb_block* b1 = (tdb_block*)a;
	tdb_block* b2 = (tdb_block*)b;

	if (b1->symbol > b2->symbol)
		return 1;
	else if (b1->symbol < b2->symbol)
		return -1;
	if (b1->ts_min > b2->ts_min)
		return 1;
	else if (b1->ts_min < b2->ts_min)
		return -1;
	if (b1->num > b2->num)
		return 1;
	else if (b1->num < b2->num)
		return -1;
	if (b1->len > b2->len)
		return 1;
	else if (b1->len < b2->len)
		return -1;

	return 0;
}

i32 tdb_write_block_index(const char* unsorted_path, tdb_block* blocks,
						  u64 num_rows) {
	char* ext = strrchr(unsorted_path, '.');
	if (ext == NULL) {
		TDB_ERRF("warning: already sorted %s", unsorted_path);
		return 0;
	}
	u64 sorted_len = ext - unsorted_path;
	char* sorted_path = (char*)malloc(sorted_len);
	if (sorted_path == NULL) {
		TDB_ERRF_SYS("malloc %lu", sorted_len);
		return 1;
	}
	memcpy(sorted_path, unsorted_path, sorted_len);
	sorted_path[sorted_len] = '\0';

	qsort(blocks, num_rows, sizeof(tdb_block), cmp_blocks);
	FILE* out = fopen(sorted_path, "w");
	if (out == NULL) {
		TDB_ERRF_SYS("open %s", sorted_path);
		free(sorted_path);
		return 2;
	}
	for (u64 i = 0; i < num_rows; i++) {
		tdb_block* b = blocks + i;
		if (b->symbol == 0 && b->ts_min == 0 && b->len == 0 && b->num == 0)
			continue;
		if (fwrite(b, 1, sizeof(tdb_block), out) == 0) {
			TDB_ERRF("fwrite %s %lu", sorted_path, i);
			free(sorted_path);
			return 3;
		}
	}

	if (remove(unsorted_path)) {
		TDB_ERRF_SYS("remove %s", unsorted_path);
		free(sorted_path);
		return 4;
	}

	if (fclose(out)) {
		TDB_ERRF_SYS("fclose %s", unsorted_path);
		free(sorted_path);
		return 5;
	}
	free(sorted_path);
	return 0;
}

static i32 tdb_table_write_block_index(tdb_table* t) {
	if (t->block_pool.used > 0) {
		pool* bp = &t->block_pool;
		u64 num_rows = t->block_pool.used / sizeof(tdb_block);
		return tdb_write_block_index(sdata(bp->file.path),
									 (tdb_block*)bp->file.data, num_rows);
	}

	return 0;
}

i32 tdb_table_close(tdb_table* t) {
	tdb_table_write_block_index(t);
	tdb_schema_free(t->schema);
	string_free(&t->data_path);

	string_free(&t->partition.name);
	hm_iter(&t->blocks) vec_free((vec_tdb_block_pool_byte_offset*)val);
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
	vec_tdb_block_pool_byte_offset* blocks = _hm_get(&t->blocks, &symbol);
	if (blocks == NULL) {
		vec_tdb_block_pool_byte_offset new_blocks = {0};
		blocks = hm_put(t->blocks, symbol, new_blocks);
	}

	for_each(offset, *blocks) {
		tdb_block* b = (tdb_block*)(t->block_pool.file.data + *offset);
		if (nanos >= b->ts_min && b->len < MIN_BLOCK_SIZE) {
			return b;
		}
	}

	tdb_block* new_block = pool_get(&t->block_pool, sizeof(tdb_block));
	new_block->symbol = symbol;
	new_block->ts_min = nanos;
	new_block->num = t->partition.num_blocks++;
	u64 used = t->block_pool.used - sizeof(tdb_block);
	vec_push_ptr(blocks, &used);

	return new_block;
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
		if (tdb_table_write_block_index(t))
			return 1;
		if (pool_close(&t->block_pool))
			return 2;
		hm_init(&t->blocks, sizeof(i32), sizeof(vec_tdb_block_pool_byte_offset),
				NULL);
		string path = string_empty;
		string_printf(&path, "%p/%p/_blocks.unsorted", &t->data_path, &p->name);
		if (pool_init(&t->block_pool, sizeof(tdb_block) * 32, sdata(path)))
			return 3;
		string_free(&path);

		// Open new columns
		for_each(col, s->columns) {
			if (mmaped_file_close(&col->file))
				return 4;
			string path = string_empty;
			string_printf(&path, "%p/%p/%p.%s", &t->data_path, &p->name,
						  &col->name, column_ext(col->type));

			if (mmaped_file_open(&col->file, sdata(path)))
				return 5;
			if (mmaped_file_resize(&col->file, COL_DEFAULT_CAP * col->stride))
				return 6;
			string_free(&path);
		}
	}

	t->block = get_block(t, id, epoch_nanos);
	tdb_col* ts_col = s->columns.data;
	tdb_table_write_data(t, &epoch_nanos, ts_col->stride);
	t->block->len += 1;

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
