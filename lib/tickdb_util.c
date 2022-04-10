#include "tickdb.h"

#include <errno.h>
#include <fcntl.h> // open
#include <linux/limits.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h> // ftruncate

#define NANOS_IN_SEC 1000000000L

static struct tm nanos_to_tm(i64 nanos) {
  nanos /= NANOS_IN_SEC;
  struct tm res;
  memcpy(&res, localtime(&nanos), sizeof(struct tm));
  return res;
}

static size_t column_stride(tdb_schema* s, tdb_coltype type) {
  switch (type) {
  case TDB_SYMBOL8:
  case TDB_INT8:
  case TDB_UINT8:
  case TDB_TIMESTAMP8:
    return 1;
  case TDB_SYMBOL16:
  case TDB_INT16:
  case TDB_UINT16:
  case TDB_TIMESTAMP16:
    return 2;
  case TDB_SYMBOL32:
  case TDB_INT32:
  case TDB_UINT32:
  case TDB_FLOAT:
  case TDB_TIMESTAMP32:
    return 4;
  case TDB_SYMBOL64:
  case TDB_CURRENCY:
  case TDB_INT64:
  case TDB_UINT64:
  case TDB_DOUBLE:
  case TDB_TIMESTAMP64:
    return 8;
  case TDB_TIMESTAMP:
    return 0;
  }
}

static const char* column_ext(tdb_table* t, tdb_coltype type) {
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
  tdb_col* columns = (tdb_col*)s->columns.data;
  for (int i = 0; i < s->columns.size; i++) {
    tdb_col* col = columns + i;

    size_t size = column_stride(s, col->type);
    if (size > res) {
      res = size;
    }
  }

  return res;
}

static inline tdb_block* get_block(tdb_table* t, i64 symbol, i64 nanos) {
  vec* blocks = _hm_get(&t->blocks, &symbol);
  if (blocks == NULL) {
    vec new_blocks = vec_init(tdb_block);
    blocks = (vec*)hm_put(&t->blocks, symbol, new_blocks);
  }

  tdb_block* data = (tdb_block*)blocks->data;
  for (size_t i = 0; i < blocks->size; i++) {
    tdb_block* b = data + i;
    if (nanos >= b->ts_min &&
        t->largest_col * b->n_rows < t->schema.block_size) {
      return b;
    }
  }

  tdb_block new_block = {
   .symbol = symbol,
   .ts_min = nanos,
  };
  return vec_push(blocks, new_block);
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
 "%U", // Week number with the first Sunday as the first day of week one
       // (00-53)	33
 "%W", // Week number with the first Monday as the first day of week one
       // (00-53)	34
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

static int days_in_month[] = {
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

static i64 min_format_specifier(string* partition_fmt, struct tm* time) {
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

static i64 min_partition_ts(tdb_table* t, i64 epoch_nanos) {
	struct tm time = nanos_to_tm(epoch_nanos);
  i64 increment = min_format_specifier(&t->schema.partition_fmt, &time);
  return epoch_nanos - epoch_nanos % increment;
}

static i64 max_partition_ts(tdb_table* t, i64 epoch_nanos) {
	struct tm time = nanos_to_tm(epoch_nanos);
  i64 increment = min_format_specifier(&t->schema.partition_fmt, &time);
  return (epoch_nanos / increment + 1) * increment;
}

static void open_column(tdb_table* t, size_t col_num) {
  tdb_col* cols = (tdb_col*)t->schema.columns.data;
  tdb_col* col = cols + col_num;

  string col_path = string_init("data/");
  string_catc(&col_path, t->partition.name);
  string_catc(&col_path, "/");
  string_cat(&col_path, &col->name);
  string_catc(&col_path, ".");
  string_catc(&col_path, column_ext(t, col->type));

  printf("open col %s\n", sdata(col_path));
  if (string_size(&col_path) > PATH_MAX) {
    fprintf(stderr, "Column file %s is longer than PATH_MAX of %d\n",
            sdata(col_path), PATH_MAX);
  }

  string builder = string_init("");
  int last_dir = 0;
  for (int i = 0; i < string_size(&col_path); i++) {
    if (sdata(col_path)[i] == '/') {
      string_catn(&builder, sdata(col_path) + last_dir, i - last_dir);
      if (mkdir(sdata(builder), S_IRWXU | S_IRWXG | S_IRWXO)) {
        if (errno != EEXIST) {
          perror(sdata(builder));
          exit(1);
        }
      }
      last_dir = i;
    }
  }
  string_free(&builder);

  int fd = open(sdata(col_path), O_RDWR);
  if (fd == -1 && errno == ENOENT) {
    fd = open(sdata(col_path), O_CREAT | O_RDWR, S_IRWXU);
    if (ftruncate(fd, GIGABYTES(1)) != 0) {
      perror(sdata(col_path));
      exit(1);
    }
  }
  if (fd == -1) {
    perror(sdata(col_path));
    exit(1);
  }

  col->data =
   mmap(NULL, GIGABYTES(1), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (col->data == MAP_FAILED) {
    string_catc(&col_path, " mmap");
    perror(sdata(col_path));
    exit(1);
  }
  string_free(&col_path);
}

static void close_columns(tdb_table* t) {
  tdb_col* cols = (tdb_col*)t->schema.columns.data;
  for (int i = 0; i < t->schema.columns.size; i++) {
    tdb_col* col = cols + i;
    if (col->data != NULL)
      munmap(col->data, col->capacity * column_stride(&t->schema, col->type));
    string_free(&col->name);
  }
}
