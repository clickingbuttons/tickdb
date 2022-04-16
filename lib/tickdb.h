#pragma once

#include "hashmap.h"
#include "schema.h"

#define TDB_MAX_FMT_LEN 64

typedef struct tdb_block {
	i64 symbol;
	i64 ts_min;
	size_t n_rows;
	size_t offset;
} tdb_block;

typedef struct tdb_partition {
	i64 ts_min;
	i64 ts_max;
	char name[TDB_MAX_FMT_LEN];
} tdb_partition;

typedef vec_t(string) vec_string;
typedef vec_t(tdb_block) vec_tdb_block;

typedef struct tdb_table {
	// schema must be first element for clever `free`ing
	tdb_schema* schema;
	size_t largest_col;
	size_t col_index;
	tdb_partition partition;
	vec_string symbols;
	// TODO: size_t sym_size;
	hashmap blocks;		 // symbol (i32): vec_tickdb_block
	hashmap symbol_uids; // symbol (char*): i32
} tdb_table;

API tdb_table* tdb_table_init(tdb_schema* s);
API i32 tdb_table_close(tdb_table* t);

API i32 tdb_table_write(tdb_table* t, char* symbol, i64 epoch_nanos);
API i32 tdb_table_write_data(tdb_table* t, void* data, size_t size);
#define register_writer(ty)                                                    \
	static i32 tdb_table_write_##ty(tdb_table* table, ty value) {             \
		return tdb_table_write_data(table, &value, sizeof(ty));                       \
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
