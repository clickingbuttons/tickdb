#pragma once

#include "util/prelude.h"
#include "util/string.h"
#include "util/vec.h"
#include "util/vecmmap.h"

typedef enum tdb_coltype {
	TDB_TIMESTAMP, // User gives us this so we can figure it out ourselves
	TDB_TIMESTAMP8,
	TDB_TIMESTAMP16,
	TDB_TIMESTAMP32,
	TDB_TIMESTAMP64,
	TDB_SYMBOL8,
	TDB_SYMBOL16,
	TDB_SYMBOL32,
	TDB_SYMBOL64,
	TDB_CURRENCY,
	TDB_INT8,
	TDB_INT16,
	TDB_INT32,
	TDB_INT64,
	TDB_UINT8,
	TDB_UINT16,
	TDB_UINT32,
	TDB_UINT64,
	TDB_FLOAT,
	TDB_DOUBLE,
} tdb_coltype;

typedef struct tdb_col {
	string name;
	tdb_coltype type;
	i64 stride;

	vec_mmap data;
} tdb_col;

typedef vec_t(tdb_col) vec_tdb_col;

typedef struct tdb_schema {
	string name;
	string ts_name;
	string partition_fmt; // strftime format
	string sym_name;
	tdb_coltype sym_type;
	string sym_universe;
	vec_tdb_col columns;
	i64 block_size;
} tdb_schema;

API tdb_schema* tdb_schema_init(char* name, char* partition_fmt,
								tdb_coltype sym_type, char* sym_universe);
API void tdb_schema_add(tdb_schema* schema, tdb_coltype type,
						char* column_name);
API void tdb_schema_free(tdb_schema* s);

// Internal
i64 min_col_stride(tdb_schema* s);
i64 max_col_stride(tdb_schema* s);
i64 column_stride(tdb_coltype type);
const char* column_ext(tdb_coltype type);
