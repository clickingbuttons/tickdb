#include "tickdb.h"
#include "string.h"

#include <fcntl.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define DEFAULT_SYM_CAPACITY 1024

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
      return 4;
    case TICKDB_SYMBOL64:
    case TICKDB_CURRENCY:
    case TICKDB_INT64:
    case TICKDB_UINT64:
    case TICKDB_DOUBLE:
      return 8;
    case TICKDB_TIMESTAMP:
      return -1;
  }
}

tickdb_table tickdb_table_init(tickdb_schema* schema) {
  tickdb_schema schema_copy;
  memcpy(&schema_copy, schema, sizeof(tickdb_schema));
  size_t sym_size = column_size(schema->sym_type);
  tickdb_table res = {
    .schema = schema_copy,

    .blocks = hm_init(sym_size, tickdb_block),
    .symbols = _vec_init(sizeof(string) * DEFAULT_SYM_CAPACITY),
    .symbol_uids = _hm_init(sizeof(string), sym_size),
  };

  return res;
}

static void open_columns(tickdb_table* table) {
  char col_path[PATH_MAX];

  for (int i = 0; i < table->schema.column_count; i++) {
    tickdb_column* col = &table->schema.columns[i];
    if (col->data == NULL) {
      int fd = open(col_path, O_RDWR);
      col->data = mmap(NULL, GIGABYTES(1), PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    }
  }
}

void tickdb_table_close(tickdb_table* table) {
  for (int i = 0; i < table->schema.column_count; i++) {
    tickdb_column* col = &table->schema.columns[i];
    if (col->data != NULL)
      munmap(col->data, col->capacity * column_size(col->type));
  }

  hm_free(&table->blocks);
  vec_free(&table->symbols);
  hm_free(&table->symbol_uids);
}

static void table_write_data(tickdb_table* table, void* data, size_t size) {
  tickdb_column* col = &table->schema.columns[table->column_index];
  memcpy((char*)(col->data) + col->length, data, size);
  col->length += size;
  table->column_index += 1;
}

static inline tickdb_block* get_block(tickdb_table* table, int64_t symbol, int64_t epoch_nanos) {
  vec* blocks = _hm_get(&table->blocks, &symbol);
  if (blocks == NULL) {
    vec new_blocks = vec_init(tickdb_block);
    blocks = (vec*)hm_put(&table->blocks, symbol, new_blocks);
  }

  tickdb_block* data = (tickdb_block*)blocks->data;
  for (size_t i = 0; i < blocks->size; i++) {
    tickdb_block* b = data + i;
    if (epoch_nanos >= b->ts_min && b->n_bytes < table->schema.block_size) {
      return b;
    }
  }

  tickdb_block new_block = {
    .symbol = symbol,
    .ts_min = epoch_nanos,
    .n_bytes = 0,
  };
  return vec_push(blocks, new_block);
}

size_t tickdb_table_stoi(tickdb_table* table, char* symbol) {
  size_t* sym = _hm_get(&table->symbol_uids, symbol);
  if (sym == NULL) {
    string s = string_init(symbol);
    vec_push(&table->symbols, s);
    sym = (size_t*)_hm_put(&table->symbol_uids, &s, &table->symbols.size);
  }
  return *sym;
}

char* tickdb_table_itos(tickdb_table* table, int64_t symbol) {
  string* symbols = (string*)table->symbols.data;
  return symbols[symbol - 1].data;
}

void tickdb_table_write(tickdb_table* table, char* symbol, int64_t epoch_nanos) {
  tickdb_block* block = get_block(table, tickdb_table_stoi(table, symbol), epoch_nanos);
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

