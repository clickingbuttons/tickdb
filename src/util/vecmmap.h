#include "prelude.h"
#include "string.h"

typedef struct vec_mmap {
	char* data;
	i64 size;
	i32 fd;
	string path;
} vec_mmap;

i32 mkdirp(const char* path);
i32 vec_mmap_grow(vec_mmap* v);
i32 vec_mmap_open(vec_mmap* v, const char* path, i64 size);
i32 vec_mmap_close(vec_mmap* v);
