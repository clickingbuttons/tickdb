#pragma once

#include "string.h"
#include "vec.h"

typedef enum tickdb_coltype {
  TDB_TIMESTAMP,
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

  // Internal
  char* data;
  size_t capacity;
  size_t size;
} tdb_col;

typedef struct tdb_schema {
  string name;
  string ts_name;
  size_t ts_stride;
  string partition_fmt; // strftime format
  string sym_name;
  tdb_coltype sym_type;
  string sym_universe;
  vec columns; // vec<tickdb_column>
  size_t block_size;
} tdb_schema;

tdb_schema tdb_schema_init(char* name, char* ts_partition_fmt,
                           tdb_coltype sym_type, char* sym_universe);
void tdb_schema_add(tdb_schema* schema, tdb_coltype type, char* column_name);
void tdb_schema_free(tdb_schema* s);