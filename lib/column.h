#pragma once

#define _GNU_SOURCE // mremap
#include "schema.h"
#include "time.h"

i32 col_grow(tdb_col* col, size_t newcap);
i32 col_open(tdb_col* col, string* table_name, const char* partition);
i32 col_close(tdb_col* col);

