#include "column.h"

#include <errno.h>
#include <fcntl.h> // open
#include <unistd.h> // ftruncate
#include <linux/limits.h> // PATH_MAX
#include <sys/mman.h> // mmap, munmap
#include <sys/stat.h> // mkdir

i32 mkdirp(const char* path) {
	string builder = string_init("");
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
		TDB_ERRF_SYS("ftruncate %s", col->path);
		return 1;
	}

	col_unmap(col);
	col->capacity = newcap;
	col->data = mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_SHARED, col->fd, 0);
	if (col->data == MAP_FAILED) {
		TDB_ERRF_SYS("mmap %s", col->path);
		return 1;
	}

	return 0;
}

i32 col_open(tdb_col* col, const char* partition) {
	size_t path_len =
	 snprintf(col->path, PATH_MAX, "data/%s/%s.%s", partition,
			  sdata(col->name), column_ext(col->type));

	if (path_len > PATH_MAX) {
		TDB_ERRF("Column file %s is longer than PATH_MAX of %d\n",
				col->path, PATH_MAX);
		return 1;
	}

	mkdirp(col->path);

	int fd = open(col->path, O_RDWR);
	if (fd == -1 && errno == ENOENT)
		fd = open(col->path, O_CREAT | O_RDWR, S_IRWXU);
	if (fd == -1) {
		TDB_ERRF("open %s", col->path);
		return 1;
	}
	col->fd = fd;
	if(col_grow(col, col->capacity)) return 1;

	return 0;
}

i32 col_close(tdb_col* col) {
	if (col_unmap(col)) return 1;
	if (col->fd) {
		if (close(col->fd)) {
			TDB_ERRF_SYS("close %s", col->path);
			return 1;
		}
	}

	return 0;
}
