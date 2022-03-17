#pragma once

#include "vec.h"

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#define TICKDB_MAX_COLUMNS 1024
#define TICKDB_MAX_COL_NAME 256

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
  char name[TICKDB_MAX_COL_NAME];
  tickdb_column_type type; 

  // Internal
  void* data;
  size_t capacity;
  size_t length;
  bool open;
} tickdb_column;

typedef struct tickdb_schema {
  char name[TICKDB_MAX_COL_NAME];
  char ts_name[TICKDB_MAX_COL_NAME];
  char partition_fmt[TICKDB_MAX_COL_NAME];
  char sym_name[TICKDB_MAX_COL_NAME];
  tickdb_column_type sym_type;
  char sym_universe[TICKDB_MAX_COL_NAME];
  tickdb_column columns[TICKDB_MAX_COLUMNS];
  size_t column_count;
  size_t column_index;
  size_t ts_size;
  size_t block_size;
} tickdb_schema;

tickdb_schema tickdb_schema_init(char* name, char *ts_partition_fmt, tickdb_column_type sym_type, char* sym_universe);
void tickdb_schema_add(tickdb_schema* schema, char* column_name, tickdb_column_type type);

typedef struct tickdb_block {
  int64_t symbol;
  int64_t ts_min;
  size_t n_bytes;
  size_t disk_offset;
} tickdb_block;

typedef struct tickdb_table {
  tickdb_schema schema;
  size_t column_index;
  vec** blocks;
  tickdb_block* block;
  size_t num_blocks;
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


