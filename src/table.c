#include "table.h"
#include "util/time.h"

#include <limits.h>

#define COL_DEFAULT_CAP 10000000
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
	res->min_col_stride = min_col_stride(s);
	res->blocks = hm_init(i32, vec_tdb_block);
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
	res->symbol_uids = hm_init(string, i32);
	res->symbol_uids.equals = hm_string_equals;
	res->symbol_uids.hasher = hm_string_hash;

	return res;
}

i32 close_block(tdb_table* t) {
	if (t->block_file != NULL) {
		if (fclose(t->block_file)) {
			TDB_ERRF_SYS("fclose %s", sdata(t->block_path));
			return 1;
		}
	}

	return 0;
}

i32 tdb_table_close(tdb_table* t) {
	tdb_schema_free(t->schema);
	string_free(&t->data_path);

	string_free(&t->partition.name);
	close_block(t);
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

static void tdb_write_block(tdb_table* t, tdb_block* b) {
	if (t->blocks.len == 1)
		fwrite("sym,ts_min,offset,n_rows", 1, 1, t->block_file);
	fwrite("\n", 1, 1, t->block_file);
	fprintf(t->block_file, "%d,%ld,%ld,%d", b->symbol, b->ts_min, b->num,
			b->n_rows);
	// fwrite(b, sizeof(*b), 1, t->block_file);
}

i32 tdb_table_stoi(tdb_table* t, char* symbol) {
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
	string* symbols = (string*)t->symbols.data;
	return sdata(symbols[symbol - 1]);
}

static tdb_block* get_block(tdb_table* t, i32 symbol, i64 nanos) {
	vec_tdb_block* blocks = _hm_get(&t->blocks, &symbol);
	if (blocks == NULL) {
		vec_tdb_block new_blocks = {0};
		blocks = hm_put(t->blocks, symbol, new_blocks);
	}

	for_each(b, *blocks) {
		if (nanos >= b->ts_min && b->n_rows * t->min_col_stride < MIN_BLOCK_SIZE) {
			b->n_rows += 1;
			return b;
		}
	}

	tdb_block new_block = {
	 .symbol = symbol,
	 .ts_min = nanos,
	 .num = blocks->len,
	 .n_rows = 1,
	};
	vec_push_ptr(blocks, &new_block);

	return blocks->data + blocks->len;
}

static bool is_old_partition(tdb_partition* p, i64 epoch_nanos) {
	if (string_len(&p->name) == 0)
		return true;
	if (epoch_nanos < p->ts_min || epoch_nanos > p->ts_max)
		return true;

	return false;
}

i32 tdb_table_write(tdb_table* t, char* symbol, i64 epoch_nanos) {
	i32 id = tdb_table_stoi(t, symbol);
	tdb_schema* s = t->schema;
	tdb_partition* p = &t->partition;
	if (is_old_partition(p, epoch_nanos)) {
		// New partition
		struct tm time = nanos_to_tm(epoch_nanos);
		string_grow(&p->name, PATH_MAX);
		p->name._size =
		 strftime(sdata(p->name), PATH_MAX, sdata(s->partition_fmt), &time);
		printf("partition %s\n", sdata(t->partition.name));

		p->ts_min = min_partition_ts(&s->partition_fmt, epoch_nanos);
		p->ts_max = max_partition_ts(&s->partition_fmt, epoch_nanos);

		// Open new block index
		close_block(t);
		string_printf(&t->block_path, "%p/%p/blocks.unsorted", &t->data_path,
					  &p->name);
		if (mkdirp(sdata(t->block_path)))
			return 1;
		t->block_file = fopen(sdata(t->block_path), "a");
		if (t->block_file == NULL) {
			TDB_ERRF_SYS("open block file %s", sdata(t->block_path));
			return 2;
		}

		// Open new columns
		for_each(col, t->schema->columns) {
			if (vec_mmap_close(&col->data))
				return 2;
      string path = string_empty;
			string_printf(&path, "data/%p/%p/%p.%s", &t->schema->name,
						  &t->partition.name, &col->name, column_ext(col->type));
			if (vec_mmap_open(&col->data, sdata(path), COL_DEFAULT_CAP,
							  col->stride))
				return 3;
      string_free(&path);
		}
	}

	t->block = get_block(t, id, epoch_nanos);
	tdb_col* ts_col = t->schema->columns.data;
	tdb_table_write_data(t, &epoch_nanos, ts_col->stride);

	return 0;
}

i32 tdb_table_write_data(tdb_table* t, void* data, i64 size) {
	tdb_col* col = (tdb_col*)t->schema->columns.data + t->col_index;
	vec_mmap* vec_mmap = &col->data;
	if (vec_mmap->len + 1 > vec_mmap->capacity)
		vec_mmap_grow(vec_mmap);
	memcpy(vec_mmap->data + vec_mmap->len * col->stride, data, size);
	vec_mmap->len += 1;
	t->col_index = (t->col_index + 1) % t->schema->columns.len;

	return 0;
}
