#pragma once

typedef enum tickdb_coltype {
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

#ifdef __cplusplus
#include <string>
#include <vector>
typedef struct tdb_col {
  std::string name;
  tdb_coltype type;

  // Internal
  char* data;
  size_t capacity;
  size_t size;
} tdb_col;

class tdb_schema {
public:
  std::string name;
  std::string ts_name;
  std::string partition_fmt; // strftime format
  std::string sym_name;
  std::string sym_universe;
  tdb_coltype sym_type;
  size_t block_size;
  std::vector<tdb_col> columns;
  size_t column_stride(tdb_coltype type);
};
#else
typedef struct tdb_schema tdb_schema;
#endif

#ifdef __cplusplus
#define TDBAPI extern "C"
#else
#define TDBAPI
#endif

TDBAPI tdb_schema* tdb_schema_init(const char* name, const char* partition_fmt,
                                   tdb_coltype sym_type,
                                   const char* sym_universe);
TDBAPI void tdb_schema_add(tdb_schema* s, tdb_coltype type,
                           const char* column_name);
TDBAPI void tdb_schema_free(tdb_schema* s);
