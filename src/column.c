#include "column.h"

#include <fcntl.h>		  // open
#include <linux/limits.h> // PATH_MAX
#include <sys/mman.h>	  // mmap, munmap, mremap
#include <sys/stat.h>	  // mkdir
#include <unistd.h>		  // ftruncate

i32 mkdirp(const char* path) {
	string builder = string_empty;
	int last_dir = 0;
	for (int i = 0;; i++) {
		if (path[i] == '/') {
			string_catn(&builder, path + last_dir, i - last_dir);
			if (mkdir(sdata(builder), S_IRWXU | S_IRWXG | S_IRWXO)) {
				if (errno != EEXIST) {
					TDB_ERRF_SYS("mkdirp: %s", sdata(builder));
					return 1;
				}
			}
			last_dir = i;
		} else if (path[i] == '\0') {
			break;
		}
	}
	string_free(&builder);
	return 0;
}

static i32 col_unmap(tdb_col* col) {
	if (col->data != NULL && munmap(col->data, col->capacity * col->stride)) {
		TDB_ERRF_SYS("munmap %s", sdata(col->name));
		return 1;
	}
	col->data = NULL;

	return 0;
}

i32 col_grow(tdb_col* col, size_t newcap) {
	size_t fsize = newcap * col->stride;
	if (ftruncate(col->fd, fsize) != 0) {
		TDB_ERRF_SYS("ftruncate %s", sdata(col->path));
		return 1;
	}

	if (col->data == NULL)
		col->data =
		 mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_PRIVATE, col->fd, 0);
	else
		col->data =
		 mremap(col->data, col->capacity * col->stride, fsize, MREMAP_MAYMOVE);
	if (col->data == MAP_FAILED) {
		TDB_ERRF_SYS("mmap %s", sdata(col->path));
		return 1;
	}
	col->capacity = newcap;

	return 0;
}

i32 col_open(tdb_col* col, string* table_name, const char* partition) {
	string_printf(&col->path, "data/%p/%s/%p.%s", table_name, partition,
				  &col->name, column_ext(col->type));
	// printf("open %s %s\n", sdata(col->name), sdata(col->path));
	if (string_len(&col->path) > PATH_MAX) {
		TDB_ERRF("Column file %s is longer than PATH_MAX of %d\n",
				 sdata(col->path), PATH_MAX);
		return 1;
	}

	if (mkdirp(sdata(col->path)))
		return 1;

	int fd = open(sdata(col->path), O_RDWR);
	if (fd == -1 && errno == ENOENT)
		fd = open(sdata(col->path), O_CREAT | O_RDWR, S_IRWXU);
	if (fd == -1) {
		TDB_ERRF("open %s", sdata(col->path));
		return 1;
	}
	col->fd = fd;
	if (col_grow(col, col->capacity))
		return 1;

	// TODO: sym column support
	return 0;
}

i32 col_close(tdb_col* col) {
	if (col_unmap(col))
		return 1;
	if (col->fd && close(col->fd)) {
		TDB_ERRF_SYS("close %s", sdata(col->path));
		return 1;
	}

	return 0;
}
