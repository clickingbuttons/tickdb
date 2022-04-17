#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "util/string.h"
#include "util/vec.h"

#include <errno.h>
#include <limits.h>

static char TDB_ERR[8096];

#define TDB_DEBUG true
#define TDB_LINENO true
#define TDB_SETERR(...) snprintf(TDB_ERR, sizeof(TDB_ERR), __VA_ARGS__)
#define TDB_PRINT_LINENO()                                                     \
	if (TDB_LINENO)                                                            \
	fprintf(stderr, "%s:%d ", __FILE__, __LINE__)
#define TDB_ERRF(...)                                                          \
	{                                                                          \
		TDB_SETERR(__VA_ARGS__);                                               \
		TDB_PRINT_LINENO();                                                    \
		if (TDB_DEBUG)                                                         \
			fprintf(stderr, "%s\n", TDB_ERR);                                  \
	}
#define TDB_ERRF_SYS(...)                                                      \
	{                                                                          \
		TDB_SETERR(__VA_ARGS__);                                               \
		TDB_PRINT_LINENO();                                                    \
		if (TDB_DEBUG)                                                         \
			fprintf(stderr, "%s: %s\n", TDB_ERR, strerror(errno));             \
	}
//#define TDB_CHECK(err) if (err != 0) fprintf(stderr, "%s\n", TDB_ERR)

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
	size_t stride;

	// Internal - data
	string path;
	int fd;
	char* data;
	size_t capacity;
	size_t len;
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
	size_t block_size;
} tdb_schema;

#define API __attribute__((__visibility__("default")))

API tdb_schema* tdb_schema_init(char* name, char* partition_fmt,
								tdb_coltype sym_type, char* sym_universe);
API void tdb_schema_add(tdb_schema* schema, tdb_coltype type,
						char* column_name);
API void tdb_schema_free(tdb_schema* s);

// Internal
size_t max_col_stride(tdb_schema* s);
size_t column_stride(tdb_coltype type);
const char* column_ext(tdb_coltype type);
