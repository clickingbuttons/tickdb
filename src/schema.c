#include "schema.h"

#define MIN_BLOCK_SIZE KIBIBYTES(16)
#define COL_DEFAULT_SIZE MEBIBYTES(250)

i64 column_stride(tdb_coltype type) {
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
		fprintf(
		 stderr,
		 "cannot know stride of TDB_TIMESTAMP. must specify TDB_TIMESTAMP64, "
		 "TDB_TIMESTAMP32, TDB_TIMESTAMP16, or TDB_TIMESTAMP8\n");
		exit(1);
	}
}

tdb_schema* tdb_schema_init(char* name, char* partition_fmt,
							tdb_coltype sym_type, char* sym_universe) {
	tdb_schema* res = calloc(sizeof(tdb_schema), 1);
	res->name = string_init(name);
	res->sym_name = string_init("sym");
	res->ts_name = string_init("ts");
	res->partition_fmt = string_init(partition_fmt);
	res->sym_universe = string_init(sym_universe);
	res->sym_type = sym_type;

	// TODO: support "resolution" which downscales "epoch_nanos"
	// >>> math.log2(24*60) Minutes
	// 10.491853096329674
	// >>> math.log2(24*60*60) Seconds
	// 16.398743691938193
	// >>> math.log2(24*60*60*10000) .1ms
	// 29.686456071487644
	tdb_coltype ts_type = TDB_TIMESTAMP64;

	size_t col_stride = column_stride(ts_type);
	tdb_col ts = {.name = string_init("ts"),
				  .type = ts_type,
				  .stride = col_stride,
				  .block_size = MIN_BLOCK_SIZE << (col_stride - 1)};
	vec_push(res->columns, ts);

	return res;
}

void tdb_schema_add(tdb_schema* s, tdb_coltype type, char* name) {
	tdb_col col = {
	 .name = string_init(name),
	 .type = type,
	 .stride = column_stride(type),
	};

	vec_push(s->columns, col);
}

void tdb_schema_free(tdb_schema* s) {
	string_free(&s->sym_name);
	string_free(&s->ts_name);
	string_free(&s->partition_fmt);
	string_free(&s->sym_universe);
	for_each(col, s->columns) {
		string_free(&col->name);
    mmaped_file_close(&col->file);
	}
	vec_free(&s->columns);
	free(s);
}

const char* column_ext(tdb_coltype type) {
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
		fprintf(
		 stderr,
		 "cannot know size of TDB_TIMESTAMP. must specify TDB_TIMESTAMP64, "
		 "TDB_TIMESTAMP32, TDB_TIMESTAMP16, or TDB_TIMESTAMP8\n");
		exit(1);
	}
}

i64 min_col_stride(tdb_schema* s) {
	i64 res = SIZE_MAX;
	for_each(col, s->columns)
		if (col->stride < res)
			res = col->stride;

	return res;
}

i64 max_col_stride(tdb_schema* s) {
	i64 res = 1;
	for_each(col, s->columns)
		if (col->stride > res)
			res = col->stride;

	return res;
}
