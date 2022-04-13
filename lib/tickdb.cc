#include "tickdb.h"
#include "util.h"

#include <errno.h>
#include <fcntl.h> // open
#include <filesystem>
#include <linux/limits.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h> // ftruncate

#define MIN_BLOCK_SIZE KIBIBYTES(64)
#define NANOS_IN_SEC 1000000000L

tdb_block* tdb_table::get_block(i64 symbol, i64 nanos) {
	// This inserts empty vec if doesn't exist
  std::vector<tdb_block>* blocs = &blocks[symbol];

  for (tdb_block& b : *blocs) {
    if (nanos >= b.ts_min) {
      return &b;
    }
  }

  blocs->push_back({
   .symbol = symbol,
   .ts_min = nanos,
  });

  return &blocs->back();
}

i64 min_partition_ts(tdb_schema* schema, i64 epoch_nanos) {
  struct tm time = nanos_to_tm(epoch_nanos);
  i64 increment = min_format_specifier(&schema->partition_fmt, &time);
  return epoch_nanos - epoch_nanos % increment;
}

i64 max_partition_ts(tdb_schema* schema, i64 epoch_nanos) {
  struct tm time = nanos_to_tm(epoch_nanos);
  i64 increment = min_format_specifier(&schema->partition_fmt, &time);
  return (epoch_nanos / increment + 1) * increment;
}

void tdb_table::open_column(size_t col_num) {
  tdb_table* t = this;
  tdb_col* col = t->schema.columns.data() + col_num;

  std::filesystem::path col_path = "data";
  col_path /= t->partition.name;
  col_path /= col->name + "." + column_ext(col->type);

  printf("open col %s\n", col_path.c_str());
  std::filesystem::create_directories(col_path.parent_path());

  int fd = open(col_path.c_str(), O_RDWR);
  if (fd == -1 && errno == ENOENT) {
    fd = open(col_path.c_str(), O_CREAT | O_RDWR, S_IRWXU);
    if (ftruncate(fd, GIGABYTES(1)) != 0) {
      perror(col_path.c_str());
      exit(1);
    }
  }
  if (fd == -1) {
    perror(col_path.c_str());
    exit(1);
  }

  col->data =
   (char*)mmap(NULL, GIGABYTES(1), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (col->data == MAP_FAILED) {
    perror(col_path.c_str());
    exit(1);
  }
}

tdb_table* tdb_table_init(tdb_schema* s) {
  tdb_table* res = new tdb_table();
  res->schema = *s;
  res->largest_col = get_largest_col_size(s);
  res->partition = {0};
  memcpy(&res->schema, s, sizeof(tdb_schema));

  return res;
}

i64 tdb_table::sym_id(const char* symbol) {
  std::string s = symbol;
  auto sym = symbol_uids.find(s);
  if (sym == symbol_uids.end()) {
    symbols.push_back(s);
    i64 size = symbols.size();
    symbol_uids[s] = size;
		return size;
  }

  return sym->second;
}

void tdb_table::write(const char* symbol, i64 epoch_nanos) {
  i64 sym = sym_id(symbol);
  tdb_block* block = get_block(sym, epoch_nanos);
  if (strlen(partition.name) == 0 || epoch_nanos < partition.ts_min ||
      epoch_nanos > partition.ts_max) {
    // Calling strftime for each row is bad perf, so instead compute min/max
    // ts's for partition
    struct tm time = nanos_to_tm(epoch_nanos);
    size_t written = strftime(partition.name, TDB_MAX_FMT_LEN,
                              schema.partition_fmt.c_str(), &time);
    if (written == 0) {
      fprintf(stderr, "partition_fmt longer than %d\n", TDB_MAX_FMT_LEN);
      exit(EXIT_FAILURE);
    }

    partition.ts_min = min_partition_ts(&schema, epoch_nanos);
    partition.ts_max = max_partition_ts(&schema, epoch_nanos);
    printf("%s %lu %lu\n", partition.name, partition.ts_min,
           partition.ts_max);

    // Close existing open columns
    close_columns();
  }

  size_t ts_stride = schema.column_stride(schema.columns[0].type);
  write_data(&epoch_nanos, ts_stride);
}

void tdb_table::write_data(void* data, size_t size) {
  tdb_col* col = schema.columns.data() + col_index;
  if (col->data == NULL) {
    open_column(col_index);
  }
  memcpy(col->data + col->size, data, size);
  col->size += size;
  col_index = (col_index + 1) % schema.columns.size();
}

const char* tdb_table::sym_string(i64 symbol) {
  return symbols[symbol - 1].c_str();
}

void tdb_table_close(tdb_table* t) { t->~tdb_table(); }

void tdb_table_write(tdb_table* t, const char* symbol, i64 epoch_nanos) {
	t->write(symbol, epoch_nanos);
}

void tdb_table_write_data(tdb_table* t, void* data, size_t size) {
	t->write_data(data, size);
}


