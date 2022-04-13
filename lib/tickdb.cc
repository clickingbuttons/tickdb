#include "tickdb.h"

#include <errno.h>
#include <fcntl.h> // open
#include <linux/limits.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h> // ftruncate
#include <filesystem>

#define MIN_BLOCK_SIZE KIBIBYTES(64)
#define NANOS_IN_SEC 1000000000L

static struct tm nanos_to_tm(i64 nanos) {
  nanos /= NANOS_IN_SEC;
  struct tm res;
  memcpy(&res, localtime(&nanos), sizeof(struct tm));
  return res;
}

static const char* column_ext(tdb_coltype type) {
  switch (type) {
  case TDB_SYMBOL8:
    return "s8";
  case TDB_INT8:
  case TDB_TIMESTAMP8:
    return "i8";
  case TDB_UINT8:
    return "u8";
  case TDB_SYMBOL16:
    return "s16";
  case TDB_INT16:
  case TDB_TIMESTAMP16:
    return "i16";
  case TDB_UINT16:
    return "u16";
  case TDB_SYMBOL32:
    return "s32";
  case TDB_INT32:
  case TDB_TIMESTAMP32:
    return "i32";
  case TDB_UINT32:
    return "u32";
  case TDB_FLOAT:
    return "f32";
  case TDB_SYMBOL64:
    return "s64";
  case TDB_CURRENCY:
    return "c64";
  case TDB_INT64:
  case TDB_TIMESTAMP64:
    return "i64";
  case TDB_UINT64:
    return "u64";
  case TDB_DOUBLE:
    return "f64";
  case TDB_TIMESTAMP:
    fprintf(stderr,
            "cannot know size of TDB_TIMESTAMP. must specify TDB_TIMESTAMP64, "
            "TDB_TIMESTAMP32, TDB_TIMESTAMP16, or TDB_TIMESTAMP8\n");
    exit(1);
  }
}

static size_t get_largest_col_size(tdb_schema* s) {
  size_t res = 1;
	for (tdb_col const& col : s->columns) {
    size_t size = s->column_stride(col.type);
    if (size > res) {
      res = size;
    }
  }

  return res;
}

static inline tdb_block* get_block(tdb_table* t, i64 symbol, i64 nanos) {
	std::vector<tdb_block>* blocks = t->blocks.get(&symbol);
  if (blocks == NULL) {
		std::vector<tdb_block> new_blocks;
    blocks = t->blocks.put(&symbol, &new_blocks);
  }

  for (tdb_block& b : *blocks) {
    if (nanos >= b.ts_min) {
      return &b;
    }
  }

	blocks->push_back({
   .symbol = symbol,
   .ts_min = nanos,
  });

	return &blocks->back();
}

static const char* second_fmts[] = {
 "%S", // Second (00-61)	02
 "%X", // Time representation *	14:55:02
 "%T", // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S	14:55:02
 "%r", // 12-hour clock time *	02:55:02 pm
};

static const char* minute_fmts[] = {
 "%M", // Minute (00-59)	55
 "%R", // 24-hour HH:MM time, equivalent to %H:%M	14:55
 "%c", // Date and time representation *	Thu Aug 23 14:55:02 2001
};

static const char* hour_fmts[] = {
 "%H", // Hour in 24h format (00-23)	14
 "%I", // Hour in 12h format (01-12)	02
};

static const char* day_fmts[] = {
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

static const char* week_fmts[] = {
 "%V", // ISO 8601 week number (01-53)	34
 "%U", // Week number with the first Sunday as the first day of week one
       // (00-53)	33
 "%W", // Week number with the first Monday as the first day of week one
       // (00-53)	34
};

static const char* month_fmts[] = {
 "%b", // Abbreviated month name *	Aug
 "%h", // Abbreviated month name * (same as %b)	Aug
 "%B", // Full month name *	August
 "%m", // Month as a decimal number (01-12)	08
};

static const char* year_fmts[] = {
 "%C", // Year divided by 100 and truncated to integer (00-99)	20
 "%g", // Week-based year, last two digits (00-99)	01
 "%G", // Week-based year	2001
 "%y", // Year, last two digits (00-99)	01
 "%Y", // Year	2001
};

static const int days_in_month[] = {
	31,
	28,
	31,
	30,
	31,
	30,
	31,
	31,
	30,
	31,
	30,
	31,
};

static bool is_leap(int year) {
	// leap year if perfectly divisible by 400
	if (year % 400 == 0) {
		return true;
	}
	// not a leap year if divisible by 100
	// but not divisible by 400
	else if (year % 100 == 0) {
		return false;
	}
	// leap year if not divisible by 100
	// but divisible by 4
	else if (year % 4 == 0) {
		return true;
	}
	// all other years are not leap years
	return false;
}

static i64 min_format_specifier(std::string* partition_fmt, struct tm* time) {
  const char* haystack = partition_fmt->c_str();
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
			u64 days = days_in_month[time->tm_mon];
			if (time->tm_mon == 1 && is_leap(time->tm_year)) {
				days += 1;
			}
      return 60 * 60 * 24 * days * NANOS_IN_SEC;
    }
  }

  // If these are the highest resolution partition format the user is doing
  // something wrong...
  // %z	ISO 8601 offset from UTC in timezone (1 minute=1, 1 hour=100) If
  // timezone cannot be determined, no characters	+100 %Z	Timezone name or
  // abbreviation * If timezone cannot be determined, no characters	CDT
  for (int i = 0; i < sizeof(year_fmts) / sizeof(year_fmts[0]); i++) {
    if (strstr(haystack, year_fmts[i]) != NULL) {
			u64 days = 365;
			if (is_leap(time->tm_year)) {
				days += 1;
			}
      return 60 * 60 * 24 * days * NANOS_IN_SEC;
    }
  }

  return 0;
}

static i64 min_partition_ts(tdb_schema* schema, i64 epoch_nanos) {
	struct tm time = nanos_to_tm(epoch_nanos);
  i64 increment = min_format_specifier(&schema->partition_fmt, &time);
  return epoch_nanos - epoch_nanos % increment;
}

static i64 max_partition_ts(tdb_schema* schema, i64 epoch_nanos) {
	struct tm time = nanos_to_tm(epoch_nanos);
  i64 increment = min_format_specifier(&schema->partition_fmt, &time);
  return (epoch_nanos / increment + 1) * increment;
}

void tdb_table::open_column(size_t col_num) {
	tdb_table* t = this;
  tdb_col* col = t->schema.columns.data() + col_num;

	std::filesystem::path col_path = "data";
	col_path /=	t->partition.name;
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

void tdb_table_close(tdb_table* t) {
	t->~tdb_table();
}

void tdb_table_write_data(tdb_table* t, void* data, size_t size) {
  tdb_col* col = t->schema.columns.data() + t->col_index;
  if (col->data == NULL) {
    t->open_column(t->col_index);
  }
  memcpy(col->data + col->size, data, size);
  col->size += size;
  t->col_index = (t->col_index + 1) % t->schema.columns.size();
}

i64 tdb_table::sym_id(const char* symbol) {
	std::string s = symbol;
  i64* sym = symbol_uids.get(&s);
  if (sym == NULL) {
		symbols.push_back(s);
		i64 size = symbols.size();
    sym = symbol_uids.put(&s, &size);
  }

	if (*sym == 0) {
		perror(symbol);
		exit(1);
	}
  return *sym;
}

const char* tdb_table::sym_string(i64 symbol) {
  return symbols[symbol - 1].c_str();
}

void tdb_table_write(tdb_table* t, char* symbol, i64 epoch_nanos) {
	i64 sym_id = t->sym_id(symbol);
  tdb_block* block = get_block(t, sym_id, epoch_nanos);
  if (strlen(t->partition.name) == 0 || epoch_nanos < t->partition.ts_min ||
      epoch_nanos > t->partition.ts_max) {
    // Calling strftime for each row is bad perf, so instead compute min/max
    // ts's for partition
    struct tm time = nanos_to_tm(epoch_nanos);
    size_t written = strftime(t->partition.name, TDB_MAX_FMT_LEN,
                              t->schema.partition_fmt.c_str(), &time);
    if (written == 0) {
      fprintf(stderr, "partition_fmt longer than %d\n",
              TDB_MAX_FMT_LEN);
      exit(EXIT_FAILURE);
    }

    t->partition.ts_min = min_partition_ts(&t->schema, epoch_nanos);
    t->partition.ts_max = max_partition_ts(&t->schema, epoch_nanos);
    printf("%s %lu %lu\n", t->partition.name, t->partition.ts_min,
           t->partition.ts_max);

    // Close existing open columns
    t->close_columns();
  }

	size_t ts_stride = t->schema.column_stride(t->schema.columns[0].type);
  tdb_table_write_data(t, &epoch_nanos, ts_stride);
}
