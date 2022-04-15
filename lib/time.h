#pragma once

#include "string.h"

#include <time.h>

#define NANOS_IN_SEC 1000000000L

struct tm nanos_to_tm(i64 nanos);
i64 min_format_specifier(string* partition_fmt, struct tm* time);

i64 min_partition_ts(string* partition_fmt, i64 epoch_nanos);
i64 max_partition_ts(string* partition_fmt, i64 epoch_nanos);

