#include "prelude.h"
#include "string.h"

typedef struct vec_mmap {
	i32 fd;
	char* data;
	i64 capacity;
	i64 len;
  i64 stride;
	string path;
} vec_mmap;

i32 mkdirp(const char* path);
i32 vec_mmap_grow(vec_mmap* v);
i32 vec_mmap_open(vec_mmap* v, string* path, i64 capacity, i64 stride);
i32 vec_mmap_close(vec_mmap* v);

