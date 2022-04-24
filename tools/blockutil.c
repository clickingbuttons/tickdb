#include "../src/table.h"

#include <stdio.h>
#include <sys/stat.h>

// want to print vector blocks tickdb_block[]
int main(int argc, char* argv[]) {
	if (argc < 2) {
		printf("usage: %s <blockfile>\n", argv[0]);
		exit(1);
	}
	FILE* f = fopen(argv[1], "r");
	if (f == NULL) {
		perror("fopen");
		exit(1);
	}

	fseek(f, 0, SEEK_END);
	u64 fsize = ftell(f);
	tdb_block* blocks = (tdb_block*)malloc(fsize);
	fseek(f, 0, SEEK_SET);
	fread(blocks, sizeof(char), fsize, f);
	u64 num_rows = fsize / sizeof(tdb_block);

	tdb_write_block_index(argv[1], blocks, num_rows);

	u64 empty_count = 0;
	bool last_empty = false;
	for (int i = 0; i < num_rows; i++) {
		tdb_block* b = blocks + i;
		if (b->symbol == 0 && b->ts_min == 0 && b->len == 0 && b->num == 0) {
			empty_count++;
			last_empty = true;
		} else {
			if (last_empty == true) {
				printf("%lu empty\n", empty_count);
				empty_count = 0;
			}
			printf("%d %lu %d %lu\n", b->symbol, b->ts_min, b->len, b->num);
			last_empty = false;
		}
	}
	if (last_empty == true) {
		printf("* %lu\n", empty_count);
		empty_count = 0;
	}

	// fprintf(stderr, "sym ts_min len num\n");
	// fprintf(stderr, "read %lu blocks\n", num_rows);
	free(blocks);
	return 0;
}
