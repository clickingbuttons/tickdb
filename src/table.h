#pragma once

#include "schema.h"
#include "util/hashmap.h"
#include "util/mmappool.h"

typedef struct tdb_block {
	i32 symbol;
	i32 len;
	i64 ts_min;
	i64 ts_max;
	i64 num; // offset into file
} tdb_block;

typedef struct tdb_partition {
	i64 ts_min;
	i64 ts_max;
	string name;
	i64 num_blocks;
} tdb_partition;

typedef vec_t(string) vec_string;
typedef vec_t(u64) vec_tdb_block_pool_byte_offset;

typedef struct tdb_table {
	// schema must be first element for clever `free`ing
	tdb_schema* schema;
	tdb_partition partition;
	string data_path;
	pool block_pool;
	hashmap blocks; // symbol (i32): vec_mmap_tickdb_block
	string schema_path;
	string symbol_path;
	FILE* symbol_file;
	vec_string symbols;
	hashmap symbol_uids; // symbol (char*): i32
	tdb_block* block;
	size_t col_index;
} tdb_table;

API tdb_table* tdb_table_init(tdb_schema* s);
API tdb_table* tdb_table_open(const char* path);
API i32 tdb_table_close(tdb_table* t);
API i32 tdb_table_flush(tdb_table* t);

API i32 tdb_table_write(tdb_table* t, const char* symbol, i64 epoch_nanos);
API i32 tdb_table_write_data(tdb_table* t, void* data, i64 size);
API i32 tdb_write_block_index(
 const char* unsorted_path,
 tdb_block* blocks,
 u64 num_rows);
#define register_writer(ty)                                                    \
	static i32 tdb_table_write_##ty(tdb_table* table, ty value) {              \
		return tdb_table_write_data(table, &value, sizeof(ty));                \
	}
register_writer(i8);
register_writer(i16);
register_writer(i32);
register_writer(i64);
register_writer(u8);
register_writer(u16);
register_writer(u32);
register_writer(u64);
register_writer(f32);
register_writer(f64);
#undef register_writer

typedef vec_t(tdb_block) vec_tdb_block;

typedef struct tdb_iter {
	size_t len;
	size_t off;
	tdb_table* table;
	vec_tdb_block blocks;
} tdb_iter;

typedef struct tdb_arr {
	char* data;
	size_t len;
} tdb_arr;

API tdb_iter* tdb_table_iter(
 tdb_table* t,
 const char* syms[],
 u64 start,
 u64 end,
 const char* cols[]);
API void tdb_iter_free(tdb_iter *it);
API static inline bool tdb_iter_next(tdb_iter* it) {
	return it->off++ > it->len;
}
tdb_arr* tdb_table_read(tdb_iter* it);

#define register_reader(ty)                                                    \
	typedef struct tdb_arr_##ty {                                              \
		ty* data;                                                              \
		size_t len;                                                            \
	} tdb_arr_##ty;                                                            \
	static tdb_arr_##ty* tdb_iter_next_##ty(tdb_iter* it) {                    \
		return (tdb_arr_##ty*)tdb_table_read(it);                              \
	}
register_reader(i8);
register_reader(i16);
register_reader(i32);
register_reader(i64);
register_reader(u8);
register_reader(u16);
register_reader(u32);
register_reader(u64);
register_reader(f32);
register_reader(f64);
#undef register_reader
