#pragma once

#include "hashmap.h"
#include "string.h"
#include "vec.h"

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#define TICKDB_MAX_COLUMNS 1024

typedef enum tickdb_column_type {
  TICKDB_TIMESTAMP,
  TICKDB_SYMBOL8,
  TICKDB_SYMBOL16,
  TICKDB_SYMBOL32,
  TICKDB_SYMBOL64,
  TICKDB_CURRENCY,
  TICKDB_INT8,
  TICKDB_INT16,
  TICKDB_INT32,
  TICKDB_INT64,
  TICKDB_UINT8,
  TICKDB_UINT16,
  TICKDB_UINT32,
  TICKDB_UINT64,
  TICKDB_FLOAT,
  TICKDB_DOUBLE,
} tickdb_column_type;

typedef struct tickdb_column {
  string name;
  tickdb_column_type type; 

  // Internal
  char* data;
  size_t capacity;
  size_t length;
} tickdb_column;

typedef struct tickdb_schema {
  string name;
  string ts_name;
  string partition_fmt; // strftime format
  string sym_name;
  tickdb_column_type sym_type;
  string sym_universe;
  vec columns; // vec<tickdb_column>
  size_t ts_size;
  size_t block_size;
} tickdb_schema;

tickdb_schema tickdb_schema_init(char* name, char *ts_partition_fmt, tickdb_column_type sym_type, char* sym_universe);
void tickdb_schema_add(tickdb_schema* schema, char* column_name, tickdb_column_type type);

typedef struct tickdb_block {
  int64_t symbol;
  int64_t ts_min;
  size_t n_rows;
  size_t offset;
} tickdb_block;

typedef struct tickdb_table {
  tickdb_schema schema;
  size_t column_index;
  hashmap blocks; // symbol (int): vec<tickdb_block>
  vec symbols; // vec<str>
  hashmap symbol_uids; // symbol (char*): int
} tickdb_table;

tickdb_table tickdb_table_init(tickdb_schema* schema);
void tickdb_table_write(tickdb_table* table, char* symbol, int64_t epoch_nanos);
void tickdb_table_write_int8(tickdb_table* table, int8_t value);
void tickdb_table_write_int16(tickdb_table* table, int16_t value);
void tickdb_table_write_int32(tickdb_table* table, int32_t value);
void tickdb_table_write_int64(tickdb_table* table, int64_t value);
void tickdb_table_write_uint8(tickdb_table* table, uint8_t value);
void tickdb_table_write_uint16(tickdb_table* table, uint16_t value);
void tickdb_table_write_uint32(tickdb_table* table, uint32_t value);
void tickdb_table_write_uint64(tickdb_table* table, uint64_t value);
void tickdb_table_write_float(tickdb_table* table, float value);
void tickdb_table_write_double(tickdb_table* table, double value);

