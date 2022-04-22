#pragma once

#include "inttypes.h"
#include "platform.h"
#include "string.h"
#include "wyhash.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HASHMAP_DEFAULT_CAPACITY 16
#define HASHMAP_LOAD_FACTOR 0.7f

typedef bool (*hashmap_equal_fn)(const void* key1, const void* key2, void* ctx);
typedef u64 (*hashmap_hash_fn)(const void* key, size_t size, void* ctx);

typedef struct hashmap {
	u64 key_size;
	u64 val_size;
	u64 capacity;
	u64 len;
	void* empty_key;
	hashmap_equal_fn equals;
	hashmap_hash_fn hasher;
  // For mmaped hashmaps
  // Use data inside of here to avoid syncing
  bool is_file;
  mmaped_file file;
} hashmap;

static inline char* hm_get_key_at(hashmap* hm, char* start, size_t index) {
	return start + index * (hm->key_size + hm->val_size);
}

static inline char* hm_get_key(hashmap* hm, size_t index) {
	return hm_get_key_at(hm, hm->file.data, index);
}

static bool hm_default_equals(const void* k1, const void* k2, void* ctx) {
	hashmap* hm = (hashmap*)ctx;
	return memcmp(k1, k2, hm->key_size) == 0;
}

static u64 hm_default_hasher(const void* key, size_t size, void* ctx) {
	return wyhash(key, size);
}

static i32 hm_init(hashmap* hm, u64 key_size, u64 val_size, const char* path) {
  memset(hm, 0, sizeof(hashmap));
  hm->key_size = key_size;
  hm->val_size = val_size;
  hm->capacity = HASHMAP_DEFAULT_CAPACITY;
  hm->equals = hm_default_equals;
  hm->hasher = hm_default_hasher;
  i64 size = (HASHMAP_DEFAULT_CAPACITY + 1) * (key_size + val_size);
  if (path == NULL) {
	  hm->file.data = (char*)malloc(size);
    if (hm->file.data == NULL) {
      TDB_ERRF_SYS("malloc %lu", size);
      return 1;
    }
    memset(hm->file.data, 0, size);
  } else {
    hm->is_file = true;
    if (mmaped_file_open(&hm->file, path))
      return 2;
    if (mmaped_file_resize(&hm->file, size))
      return 3;
  }

	hm->empty_key = hm_get_key(hm, hm->capacity);
	return 0;
}

static void hm_grow(hashmap* hm);

static inline char* hm_get_val(hashmap* hm, size_t index) {
	return hm_get_key(hm, index) + hm->key_size;
}

static void* _hm_put(hashmap* hm, void* key, void* val) {
	u64 index = hm->hasher(key, hm->key_size, NULL) & (hm->capacity - 1);

	if (hm->equals(hm->empty_key, key, hm)) {
		fprintf(stderr, "cannot write 0 key\n");
		exit(1);
	}

	if ((hm->len + 1) > HASHMAP_LOAD_FACTOR * hm->capacity)
		hm_grow(hm);

	char* existing_key = hm->file.data + index * (hm->key_size + hm->val_size);
	if (hm->equals(existing_key, key, hm)) { // overwrite
		memcpy(existing_key + hm->key_size, val, hm->val_size);
		return existing_key + hm->key_size;
	} else { // insert
		char* offset = hm_get_key(hm, index);
		for (int i = 1; !hm->equals(offset, hm->empty_key, hm); i++) {
			index += i * i;
			index = index & (hm->capacity - 1);
			offset = hm_get_key(hm, index);
		}
		memcpy(offset, key, hm->key_size);
		memcpy(offset + hm->key_size, val, hm->val_size);
		hm->len += 1;
		return offset + hm->key_size;
	}
}

#define hm_put(hm, key, val)                                                   \
	({                                                                         \
		typeof(key) key_copy = key;                                            \
		typeof(val) val_copy = val;                                            \
		_hm_put(&hm, &key_copy, &val_copy);                                    \
	})

static void hm_grow(hashmap* hm) {
	size_t old_capacity = hm->capacity;
	char* old_data = hm->file.data;
	hm->capacity *= 2;
  i64 size = (hm->capacity + 1) * (hm->key_size + hm->val_size);
  if (hm->is_file) {
    // TODO: better mmaped_file growth strategy
    old_data = (char*)calloc(old_capacity, hm->key_size + hm->val_size);
    memcpy(old_data, hm->file.data, old_capacity);
    mmaped_file_resize(&hm->file, size);
    memset(hm->file.data, 0, size);
  }
  else {
    hm->file.data = (char*)calloc(hm->capacity + 1, hm->key_size + hm->val_size);
  }
	hm->empty_key = hm_get_key(hm, hm->capacity);
	hm->len = 0;
	for (int i = 0; i < old_capacity; i++) {
		char* key = hm_get_key_at(hm, old_data, i);
		if (hm->equals(key, hm->empty_key, hm))
			continue;
		_hm_put(hm, key, key + hm->key_size);
	}
	free(old_data);
}

static void* _hm_get(hashmap* hm, void* key) {
	u64 index = hm->hasher(key, hm->key_size, NULL) & (hm->capacity - 1);
	char* offset = hm_get_key(hm, index);
	if (hm->equals(offset, hm->empty_key, hm))
		return NULL;
	for (u64 i = 1; !hm->equals(offset, key, hm); i++) {
		index += i * i;
		index = index & (hm->capacity - 1);
		offset = hm_get_key(hm, index);
		if (hm->equals(offset, hm->empty_key, hm))
			return NULL;
	}

	return offset + hm->key_size;
}

#define hm_get(hm, key, valtype)                                               \
	({                                                                         \
		typeof(key) key_copy = key;                                            \
		*((valtype*)_hm_get(&hm, &key_copy));                                  \
	})

static i32 hm_free(hashmap* hm) {
  if (hm->is_file)
    return mmaped_file_close(&hm->file);
  else
    free(hm->file.data);

  return 0;
}

static void hm_print(hashmap* hm) {
	for (int i = 0; i < hm->capacity; i++) {
		char* key = hm_get_key(hm, i);
		if (hm->equals(key, hm->empty_key, hm))
			continue;
		printf("%p ", key);
		for (int j = 0; j < hm->key_size; j++) {
			printf("%02hhx", *(key + j));
			if (j != hm->key_size - 1)
				printf(" ");
		}
		printf("/");
		for (int j = 0; j < hm->val_size; j++) {
			printf("%02hhx", *(key + hm->key_size + j));
			if (j != hm->val_size - 1)
				printf(" ");
		}
		printf("\n");
	}
}

static char* hm_begin(hashmap* hm) { return hm->file.data; }

static char* hm_end(hashmap* hm) {
	return hm->file.data + hm->capacity * (hm->key_size + hm->val_size);
}

static char* hm_next(hashmap* hm, char* cur) {
	char* it;
	for (it = cur; it < hm_end(hm); it += hm->key_size + hm->val_size) {
		if (hm->equals(hm->empty_key, it, hm))
			continue;
		return it;
	}

	return it;
}

#define hm_iter(hm)                                                            \
	for (char *it = hm_next(hm, hm_begin(hm)), *key = it,                      \
			  *val = it + (hm)->key_size;                                      \
		 it != hm_end(hm);                                                     \
		 it = hm_next(hm, it + (hm)->key_size + (hm)->val_size), key = it,     \
			  val = it + (hm)->key_size)

#ifdef TEST_HM
int main(void) {
	hashmap m = hm_init(i32, i32);
	for (int i = 1; i < 70; i++) {
		hm_put(m, i, i * i);
	}
	hm_print(&m);
	int i = 0;
	hm_iter(&m) {
		printf("%d %d\n", *(i32*)key, *(i32*)val);
		i++;
	}
	printf("sizes %d %d\n", i, m.len);
}
#endif
