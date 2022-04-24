#pragma once

#include "schema.h"
#include "util/hashmap.h"
#include "util/mmappool.h"

typedef struct tdb_block {
	i32 symbol;
	i32 len;
	i64 ts_min;
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
	string symbol_path;
	FILE* symbol_file;
	vec_string symbols;
	hashmap symbol_uids; // symbol (char*): i32
	tdb_block* block;
	size_t col_index;
} tdb_table;

API tdb_table* tdb_table_init(tdb_schema* s);
API i32 tdb_table_close(tdb_table* t);

API i32 tdb_table_write(tdb_table* t, const char* symbol, i64 epoch_nanos);
API i32 tdb_table_write_data(tdb_table* t, void* data, i64 size);
API i32 tdb_write_block_index(const char* unsorted_path, tdb_block* blocks,
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
