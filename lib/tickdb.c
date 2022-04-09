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
#include <unistd.h>

#define DEFAULT_SYM_CAPACITY 1024
#define MIN_BLOCK_SIZE KIBIBYTES(64)
#define NANOS_IN_SEC 1000000000L

tickdb_schema tickdb_schema_init(char* name, char *ts_partition_fmt, tickdb_column_type sym_type, char* sym_universe) {
  tickdb_schema res = {
    .name = string_init(name),
    .sym_name = string_init("sym"),
    .ts_name = string_init("ts"),
    .ts_stride = 8, // TODO: optimize lower resolutions
    .partition_fmt = string_init(ts_partition_fmt),
    .sym_universe = string_init(sym_universe),
    .columns = vec_init(tickdb_column),
    .sym_type = sym_type,
  };

  tickdb_column ts = {
    .name = string_init("ts"),
    .type = TICKDB_TIMESTAMP,
  };
  vec_push(&res.columns, ts);

  return res;
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

static void open_column(tickdb_table* table, size_t col_num) {
  tickdb_column* cols = (tickdb_column*) table->schema.columns.data;
  tickdb_column* col = cols + col_num;

  string col_path = string_init("data/");
  string_catc(&col_path, table->cur_partition.name);
  string_catc(&col_path, "/");
  string_cat(&col_path, &col->name);
  string_catc(&col_path, ".");
  string_catc(&col_path, column_ext(col->type));

  printf("open col %s\n", string_data(&col_path));
  if (string_size(&col_path) > PATH_MAX) {
    fprintf(stderr, "Column file %s is longer than PATH_MAX of %d\n", string_data(&col_path), PATH_MAX);
  }

  string builder = string_init("");
  int last_dir = 0;
  for (int i = 0; i < string_size(&col_path); i++) {
    if (string_data(&col_path)[i] == '/') {
      string_catn(&builder, string_data(&col_path) + last_dir, i - last_dir);
      if (mkdir(string_data(&builder), S_IRWXU | S_IRWXG | S_IRWXO)) {
        if (errno != EEXIST) {
          perror(string_data(&builder));
          exit(1);
        }
      }
      last_dir = i;
    }
  }
  int fd = open(string_data(&col_path), O_RDWR);
  if (fd == -1 && errno == ENOENT) {
    fd = open(string_data(&col_path), O_CREAT | O_RDWR, S_IRWXU);
    if (ftruncate(fd, GIGABYTES(1)) != 0) {
      perror(string_data(&col_path));
      exit(1);
    }
  }
  if (fd == -1) {
    perror(string_data(&col_path));
    exit(1);
  }

  col->data = mmap(NULL, GIGABYTES(1), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (col->data == MAP_FAILED) {
    string_catc(&col_path, " mmap");
    perror(string_data(&col_path));
    exit(1);
  }
  printf("data %p\n", col->data);
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
    .cur_partition = { 0 },
    .blocks = hm_init(sym_size, tickdb_block),
    .symbols = vec_init(string),
    .symbol_uids = _hm_init(sizeof(string), sym_size),
  };

  return res;
}

static void close_columns(tickdb_table* table) {
  tickdb_column* cols = (tickdb_column*) table->schema.columns.data;
  for (int i = 0; i < table->schema.columns.size; i++) {
    tickdb_column* col = cols + i;
    if (col->data != NULL)
      munmap(col->data, col->capacity * column_size(col->type));
  }
}

void tickdb_table_close(tickdb_table* table) {
  close_columns(table);

  hm_free(&table->blocks);
  vec_free(&table->symbols);
  hm_free(&table->symbol_uids);
}

static void table_write_data(tickdb_table* table, void* data, size_t size) {
  tickdb_column* cols = (tickdb_column*) table->schema.columns.data;
  tickdb_column* col = cols + table->cur_col_index;
  if (col->data == NULL) {
    open_column(table, table->cur_col_index);
  }
  memcpy(col->data + col->size, data, size);
  col->size += size;
  table->cur_col_index = (table->cur_col_index + 1) % table->schema.columns.size;
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
  string s = string_init(symbol);
  size_t* sym = _hm_get(&table->symbol_uids, &s);
  if (sym == NULL) {
    _vec_push(&table->symbols, &s);
    sym = (size_t*)_hm_put(&table->symbol_uids, &s, &table->symbols.size);
  }
  return *sym;
}

char* tickdb_table_itos(tickdb_table* table, int64_t symbol) {
  string* symbols = (string*)table->symbols.data;
  return string_data(&symbols[symbol - 1]);
}

static char* second_fmts[] = {
  "%S", // Second (00-61)	02
  "%X", // Time representation *	14:55:02
  "%T", // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S	14:55:02
  "%r", // 12-hour clock time *	02:55:02 pm
};

static char* minute_fmts[] = {
  "%M", // Minute (00-59)	55
  "%R", // 24-hour HH:MM time, equivalent to %H:%M	14:55
  "%c", // Date and time representation *	Thu Aug 23 14:55:02 2001
};

static char* hour_fmts[] = {
  "%H", // Hour in 24h format (00-23)	14
  "%I", // Hour in 12h format (01-12)	02
};

static char* day_fmts[] = {
  "%j", // Day of the year (001-366)	235
  "%d", // Day of the month, zero-padded (01-31)	23
  "%e", // Day of the month, space-padded ( 1-31)	23
  "%x", // Date representation *	08/23/01
  "%a", // Abbreviated weekday name *	Thu
  "%A", // Full weekday name *	Thursday
  "%u", // ISO 8601 weekday as number with Monday as 1 (1-7)	4
  "%w", // Weekday as a decimal number with Sunday as 0 (0-6)	4
  "%D", // Short MM/DD/YY date, equivalent to %m/%d/%y	08/23/01
  "%F", // Short YYYY-MM-DD date, equivalent to %Y-%m-%d	2001-08-23
};

static char* week_fmts[] = {
  "%V", // ISO 8601 week number (01-53)	34
  "%U", // Week number with the first Sunday as the first day of week one (00-53)	33
  "%W", // Week number with the first Monday as the first day of week one (00-53)	34
};

static char* month_fmts[] = {
  "%b", // Abbreviated month name *	Aug
  "%h", // Abbreviated month name * (same as %b)	Aug
  "%B", // Full month name *	August
  "%m", // Month as a decimal number (01-12)	08
};

static char* year_fmts[] = {
  "%C", // Year divided by 100 and truncated to integer (00-99)	20
  "%g", // Week-based year, last two digits (00-99)	01
  "%G", // Week-based year	2001
  "%y", // Year, last two digits (00-99)	01
  "%Y", // Year	2001
};

int64_t min_format_specifier(string* partition_fmt) {
  char* haystack = string_data(partition_fmt);
  for (int i = 0; i < sizeof(second_fmts) / sizeof(second_fmts[0]); i++) {
    if (strstr(haystack, second_fmts[i]) != NULL) {
      return NANOS_IN_SEC;
    }
  }

  for (int i = 0; i < sizeof(minute_fmts) / sizeof(minute_fmts[0]); i++) {
    if (strstr(haystack, minute_fmts[i]) != NULL) {
      return 60 * NANOS_IN_SEC;
    }
  }

  for (int i = 0; i < sizeof(hour_fmts) / sizeof(hour_fmts[0]); i++) {
    if (strstr(haystack, hour_fmts[i]) != NULL) {
      return 60 * 60 * NANOS_IN_SEC;
    }
  }

  if (strstr(haystack, "%p") != NULL) { // AM or PM designation
    return 60 * 60 * 12 * NANOS_IN_SEC;
  }

  for (int i = 0; i < sizeof(day_fmts) / sizeof(day_fmts[0]); i++) {
    if (strstr(haystack, day_fmts[i]) != NULL) {
      return 60 * 60 * 24 * NANOS_IN_SEC;
    }
  }

  for (int i = 0; i < sizeof(month_fmts) / sizeof(month_fmts[0]); i++) {
    if (strstr(haystack, month_fmts[i]) != NULL) {
      return 60 * 60 * 24 * 30 * NANOS_IN_SEC;
    }
  }

  // If these are the highest resolution partition format the user is doing something wrong...
  //%z	ISO 8601 offset from UTC in timezone (1 minute=1, 1 hour=100) If timezone cannot be determined, no characters	+100
  //%Z	Timezone name or abbreviation * If timezone cannot be determined, no characters	CDT

  for (int i = 0; i < sizeof(year_fmts) / sizeof(year_fmts[0]); i++) {
    if (strstr(haystack, year_fmts[i]) != NULL) {
      return 60 * 60 * 24 * 365 * NANOS_IN_SEC;
    }
  }

  return 0;
}

int64_t min_partition_ts(tickdb_table* table, int64_t epoch_nanos) {
  // TODO: month and leap year maths
  int64_t increment = min_format_specifier(&table->schema.partition_fmt);
  return epoch_nanos - epoch_nanos % increment;
}

int64_t max_partition_ts(tickdb_table* table, int64_t epoch_nanos) {
  // TODO: month and leap year maths
  int64_t increment = min_format_specifier(&table->schema.partition_fmt);
  return (epoch_nanos / increment + 1) * increment;
}

void tickdb_table_write(tickdb_table* table, char* symbol, int64_t epoch_nanos) {
  tickdb_block* block = get_block(table, tickdb_table_stoi(table, symbol), epoch_nanos);
  if (
    strlen(table->cur_partition.name) == 0 ||
    epoch_nanos < table->cur_partition.ts_min ||
    epoch_nanos > table->cur_partition.ts_max
  ) {
    // Calling strftime for each row is bad perf, so instead compute min/max ts's for partition
    struct tm time = nanos_to_tm(epoch_nanos);
    size_t written = strftime(
      table->cur_partition.name,
      TICKDB_MAX_PARTITIONFMT_LEN,
      string_data(&table->schema.partition_fmt),
      &time
    );
    if (written == 0) {
      fprintf(stderr, "partition_fmt longer than %d\n", TICKDB_MAX_PARTITIONFMT_LEN);
      exit(EXIT_FAILURE);
    }

    table->cur_partition.ts_min = min_partition_ts(table, epoch_nanos);
    table->cur_partition.ts_max = max_partition_ts(table, epoch_nanos);
    printf("%s %lu %lu\n", table->cur_partition.name, table->cur_partition.ts_min, table->cur_partition.ts_max);

    // Close existing open columns
    close_columns(table);
  }

  // TODO: efficiently support lower resolutions than nanos
  table_write_data(table, &epoch_nanos, table->schema.ts_stride);
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

