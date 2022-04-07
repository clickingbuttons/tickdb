#include "tickdb.h"
#include "string.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>

#define MAX_USER_PARTITIONFORMAT 64
#define DEFAULT_SYM_CAPACITY 1024
#define MIN_BLOCK_SIZE KIBIBYTES(64)
#define NANOS_IN_SEC 1000000000

tickdb_schema tickdb_schema_init(char* name, char *ts_partition_fmt, tickdb_column_type sym_type, char* sym_universe) {
  return (tickdb_schema) {
    .name = string_init(name),
    .sym_name = string_init("sym"),
    .ts_name = string_init("ts"),
    .partition_fmt = string_init(ts_partition_fmt),
    .sym_universe = string_init(sym_universe),
    .columns = vec_init(tickdb_column),
    .sym_type = sym_type,
  };
}

void tickdb_schema_add(tickdb_schema* schema, char* name, tickdb_column_type type) {
  tickdb_column col = {
    .name = string_init(name),
    .type = type,
  };

  vec_push(&schema->columns, col);
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

static const char* column_ext(tickdb_column_type type) {
  switch (type) {
    case TICKDB_SYMBOL8:
      return "s8";
    case TICKDB_INT8:
      return "i8";
    case TICKDB_UINT8:
      return "u8";
    case TICKDB_SYMBOL16:
      return "s16";
    case TICKDB_INT16:
      return "i16";
    case TICKDB_UINT16:
      return "u16";
    case TICKDB_SYMBOL32:
      return "s32";
    case TICKDB_INT32:
      return "i32";
    case TICKDB_UINT32:
      return "u32";
    case TICKDB_FLOAT:
      return "f32";
    case TICKDB_SYMBOL64:
      return "s64";
    case TICKDB_CURRENCY:
      return "c64";
    case TICKDB_INT64:
      return "i64";
    case TICKDB_UINT64:
      return "u64";
    case TICKDB_DOUBLE:
      return "f64";
    case TICKDB_TIMESTAMP:
      // TODO: right size based on table
      return "ts";
  }
}

static struct tm nanos_to_tm(int64_t nanos) {
  nanos /= NANOS_IN_SEC;
  struct tm res;
  memcpy(&res, localtime(&nanos), sizeof(struct tm));
  return res;
}

static void open_column(tickdb_table* table, size_t col_num, int64_t nanos) {
  tickdb_column* cols = (tickdb_column*) table->schema.columns.data;
  tickdb_column* col = cols + col_num;

  char usertime[MAX_USER_PARTITIONFORMAT];
  struct tm time = nanos_to_tm(nanos);
  size_t usertimelen = strftime(usertime, MAX_USER_PARTITIONFORMAT, string_data(&table->schema.partition_fmt), &time);

  string col_path = string_init("data/");
  string_catn(&col_path, usertime, usertimelen);
  string_catc(&col_path, "/");
  string_cat(&col_path, &col->name);
  string_catc(&col_path, ".");
  string_catc(&col_path, column_ext(col->type));

  printf("open col %s\n", string_data(&col_path));
  if (string_size(&col_path) > PATH_MAX) {
    fprintf(stderr, "Column file %s is longer than PATH_MAX of %d\n", string_data(&col_path), PATH_MAX);
  }

  string buildup = string_init("");
  int last_dir = 0;
  for (int i = 0; i < string_size(&col_path); i++) {
    if (string_data(&col_path)[i] == '/') {
      string_catn(&buildup, string_data(&col_path) + last_dir, i - last_dir);
      if (mkdir(string_data(&buildup), S_IRWXU | S_IRWXG | S_IRWXO)) {
        if (errno != EEXIST) {
          perror(string_data(&buildup));
          exit(1);
        }
      }
      last_dir = i;
    }
  }
  int fd = open(string_data(&col_path), O_CREAT | O_RDWR, S_IRWXU);
  if (fd == -1) {
    perror(string_data(&col_path));
    exit(1);
  }

  col->data = mmap(NULL, GIGABYTES(1), PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
  if (col->data == MAP_FAILED) {
    string_catc(&col_path, " mmap");
    perror(string_data(&col_path));
    exit(1);
  }
}

static size_t get_largest_col_size(tickdb_schema* schema) {
  size_t res = 1;
  tickdb_column* columns = (tickdb_column*) schema->columns.data;
  for (int i = 0; i < schema->columns.size; i++) {
    tickdb_column* col = columns + i;

    size_t size = column_size(col->type);
    if (size > res) {
      res = size;
    }
  }

  return res;
}

tickdb_table tickdb_table_init(tickdb_schema* schema) {
  tickdb_schema schema_copy;
  memcpy(&schema_copy, schema, sizeof(tickdb_schema));
  size_t sym_size = column_size(schema->sym_type);
  tickdb_table res = {
    .schema = schema_copy,
    .largest_col = get_largest_col_size(schema),

    .blocks = hm_init(sym_size, tickdb_block),
    .symbols = _vec_init(sizeof(string) * DEFAULT_SYM_CAPACITY),
    .symbol_uids = _hm_init(sizeof(string), sym_size),
  };
  open_column(&res, 0, 1649114365L * NANOS_IN_SEC);

  return res;
}

void tickdb_table_close(tickdb_table* table) {
  tickdb_column* cols = (tickdb_column*) table->schema.columns.data;
  for (int i = 0; i < table->schema.columns.size; i++) {
    tickdb_column* col = cols + i;
    if (col->data != NULL)
      munmap(col->data, col->capacity * column_size(col->type));
  }

  hm_free(&table->blocks);
  vec_free(&table->symbols);
  hm_free(&table->symbol_uids);
}

static void table_write_data(tickdb_table* table, void* data, size_t size) {
  tickdb_column* cols = (tickdb_column*) table->schema.columns.data;
  tickdb_column* col = cols + table->column_index;
  memcpy(col->data + col->length, data, size);
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
    if (epoch_nanos >= b->ts_min && table->largest_col * b->n_rows < table->schema.block_size) {
      return b;
    }
  }

  tickdb_block new_block = {
    .symbol = symbol,
    .ts_min = epoch_nanos,
  };
  return vec_push(blocks, new_block);
}

size_t tickdb_table_stoi(tickdb_table* table, char* symbol) {
  size_t* sym = _hm_get(&table->symbol_uids, symbol);
  if (sym == NULL) {
    string s = string_init(symbol);
    _vec_push(&table->symbols, &s);
    sym = (size_t*)_hm_put(&table->symbol_uids, &s, &table->symbols.size);
  }
  return *sym;
}

char* tickdb_table_itos(tickdb_table* table, int64_t symbol) {
  string* symbols = (string*)table->symbols.data;
  return string_data(&symbols[symbol - 1]);
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

