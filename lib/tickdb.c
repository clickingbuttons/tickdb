#include "tickdb.h"

#include <fcntl.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define GIGABYTES(amount) amount * 1000 * 1000 * 1000

tickdb_schema tickdb_schema_init(char* name, char *ts_partition_fmt, tickdb_column_type sym_type, char* sym_universe) {
  tickdb_schema res = {
    // Defaults
    .sym_name = "sym",
    .ts_name = "ts",
    .column_count = 0,

    .sym_type = sym_type,
  };
  strcpy(res.name, name);
  strcpy(res.partition_fmt, ts_partition_fmt);
  strcpy(res.sym_universe, sym_universe);

  return res;
}

void tickdb_schema_add(tickdb_schema* schema, char* name, tickdb_column_type type) {
  tickdb_column* col = &schema->columns[schema->column_count];
  strcpy(col->name, name);
  col->type = type;
  col->capacity = 0;
  col->length = 0;
  schema->column_count += 1;
}

tickdb_table tickdb_table_init(tickdb_schema* schema) {
  tickdb_schema schema_copy;
  memcpy(&schema_copy, schema, sizeof(tickdb_schema));
  tickdb_table res = {
    .schema = schema_copy,

    .blocks = NULL,
    .num_blocks = 0,
  };

  return res;
}

static size_t column_size(tickdb_column_type type) {
  switch (type) {
    case TICKDB_SYMBOL8:
    case TICKDB_INT8:
    case TICKDB_UINT8:
      return 1;
    case TICKDB_SYMBOL16:
    case TICKDB_INT16:
    case TICKDB_UINT16:
      return 2;
    case TICKDB_SYMBOL32:
    case TICKDB_INT32:
    case TICKDB_UINT32:
    case TICKDB_FLOAT:
      return 2;
    case TICKDB_SYMBOL64:
    case TICKDB_CURRENCY:
    case TICKDB_INT64:
    case TICKDB_UINT64:
    case TICKDB_DOUBLE:
      return 4;
    case TICKDB_TIMESTAMP:
      return -1;
  }
}

static void open_columns(tickdb_table* table) {
  char col_path[PATH_MAX];

  for (int i = 0; i < table->schema.column_count; i++) {
    tickdb_column* col = &table->schema.columns[i];
    if (!col->open) {
      int fd = open(col_path, O_RDWR);
      col->data = mmap(NULL, GIGABYTES(1), PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    }
  }
}

void tickdb_table_close(tickdb_table* table) {
  for (int i = 0; i < table->schema.column_count; i++) {
    tickdb_column* col = &table->schema.columns[i];
    if (col->open)
      munmap(col->data, col->capacity * column_size(col->type));
  }

  for (int i = 0; i < table->num_blocks; i++)
    free(table->blocks[i]);
  table->num_blocks = 0;
}

static void table_write_data(tickdb_table* table, void* data, size_t size) {
  tickdb_column* col = &table->schema.columns[table->column_index];
  memcpy((char*)(col->data) + col->length, data, size);
  col->length += size;
  table->column_index += 1;
}

static inline tickdb_block* get_block(tickdb_table* table, int64_t symbol, int64_t epoch_nanos) {
  for (size_t i = 0; i < table->num_blocks; i++) {
    tickdb_block* b = table->blocks[i];
    if (b->symbol == symbol && epoch_nanos >= b->ts_min && b->n_bytes < table->schema.block_size) {
      return b;
    }
  }

  tickdb_block* newBlock = malloc(sizeof(tickdb_block));
  newBlock->symbol = symbol;
  newBlock->ts_min = epoch_nanos;
  newBlock->n_bytes = 0;

  return newBlock;
}

size_t tickdb_table_stoi(tickdb_table* table, char* symbol) {
  // TODO: hashmap
  return 0;
}

char* tickdb_table_itos(tickdb_table* table, int64_t symbol) {
  // TODO: vector
  return "asdf";
}

void tickdb_table_write(tickdb_table* table, char* symbol, int64_t epoch_nanos) {
  table->block = get_block(table, tickdb_table_stoi(table, symbol), epoch_nanos);
  // TODO: convert epoch_nanos based on time clamps
  table_write_data(table, &epoch_nanos, table->schema.ts_size);
}
void tickdb_table_write_int8(tickdb_table* table, int8_t value) {
  table_write_data(table, &value, 1);
}
void tickdb_table_write_int16(tickdb_table* table, int16_t value) {
  table_write_data(table, &value, 2);
}
void tickdb_table_write_int32(tickdb_table* table, int32_t value) {
  table_write_data(table, &value, 4);
}
void tickdb_table_write_int64(tickdb_table* table, int64_t value) {
  table_write_data(table, &value, 8);
}
void tickdb_table_write_uint8(tickdb_table* table, uint8_t value) {
  table_write_data(table, &value, 1);
}
void tickdb_table_write_uint16(tickdb_table* table, uint16_t value) {
  table_write_data(table, &value, 2);
}
void tickdb_table_write_uint32(tickdb_table* table, uint32_t value) {
  table_write_data(table, &value, 4);
}
void tickdb_table_write_uint64(tickdb_table* table, uint64_t value) {
  table_write_data(table, &value, 8);
}
void tickdb_table_write_float(tickdb_table* table, float value) { 
  table_write_data(table, &value, 4);
}
void tickdb_table_write_double(tickdb_table* table, double value) {
  table_write_data(table, &value, 8);
}

