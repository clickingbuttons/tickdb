#pragma once

#include "schema.h"
#include "time.h"

i32 col_grow(tdb_col* col, size_t newcap);
i32 col_open(tdb_col* col, const char* partition);
i32 col_close(tdb_col* col);

