#pragma once

#include "inttypes.h"
#include "wyhash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HASHMAP_DEFAULT_CAPACITY 16
#define HASHMAP_LOAD_FACTOR 0.7f

typedef struct hashmap {
  size_t key_size;
  size_t val_size;
  size_t capacity;
  size_t nmemb;
  void* zero_key; // used for testing if row is available
  char* data;
} hashmap;

#define hm_init(key_type, val_type) _hm_init(sizeof(key_type), sizeof(val_type))

static hashmap _hm_init(size_t key_size, size_t val_size) {
  hashmap res = {
    .key_size = key_size,
    .val_size = val_size,
    .capacity = HASHMAP_DEFAULT_CAPACITY,
    .nmemb = 0,
    .zero_key = calloc(1, key_size),
    .data = (char*)calloc(HASHMAP_DEFAULT_CAPACITY, key_size + val_size),
  };
  return res;
}

static void hm_grow(hashmap* hm);

#define hm_put(hm, key, val) { \
  typeof(key) key_copy = key;  \
  typeof(val) val_copy = val;  \
  _hm_put(hm, &key_copy, &val_copy);     \
}

static inline char* hm_get_key_at(hashmap* hm, char* start, size_t index) {
  return start + index * (hm->key_size + hm->val_size);
}

static inline char* hm_get_key(hashmap* hm, size_t index) {
  return hm_get_key_at(hm, hm->data, index);
}

static inline char* hm_get_val(hashmap* hm, size_t index) {
  return hm_get_key(hm, index) + hm->key_size;
}

static inline char* hm_find_key_at(hashmap* hm, void* key, size_t start) {
  char* res = hm_get_key(hm, start);
  for (int i = 1; memcmp(res, key, hm->key_size) != 0; i++) {
    start += i * i;
    start = start & (hm->capacity - 1);
    res = hm_get_key(hm, start);
  }

  return res;
}

static void _hm_put(hashmap* hm, void* key, void* val) {
  uint64_t index = wyhash(key, hm->key_size) & (hm->capacity - 1);

  if (memcmp(hm->zero_key, key, hm->key_size) == 0) {
    perror("cannot write 0 key");
    return;
  }

  if ((hm->nmemb + 1) > HASHMAP_LOAD_FACTOR * hm->capacity) {
    hm_grow(hm);
  }

  char* existing_key = hm->data + index * (hm->key_size + hm->val_size);
  if (memcmp(existing_key, key, hm->key_size) == 0) { // overwrite
    memcpy(existing_key + hm->key_size, val, hm->val_size);
  } else { // insert
    char* offset = hm_find_key_at(hm, hm->zero_key, index);
    memcpy(offset, key, hm->key_size);
    memcpy(offset + hm->key_size, val, hm->val_size);
    hm->nmemb += 1;
  }
}

static void hm_grow(hashmap* hm) {
  size_t old_capacity = hm->capacity;
  char* old_data = hm->data;
  hm->capacity *= 2;
  hm->data = (char*)calloc(hm->capacity, hm->key_size + hm->val_size);
  for (int i = 0; i < old_capacity; i++) {
    char* key = hm_get_key_at(hm, old_data, i);
    if (memcmp(key, hm->zero_key, hm->key_size) == 0) {
      continue;
    }
    _hm_put(hm, key, key + hm->key_size);
  }
  free(old_data);
}

#define hm_get(hm, key, valtype) ({ \
  typeof(key) key_copy = key;  \
  *((valtype*) _hm_get(hm, &key_copy));     \
})

static void* _hm_get(hashmap* hm, void* key) {
  uint64_t index = wyhash(key, hm->key_size) & (hm->capacity - 1);
  char* offset = hm_find_key_at(hm, key, index);

  return offset + hm->key_size;
}

static void hm_free(hashmap* hm) {
  free(hm->data);
  free(hm->zero_key);
}

static void hm_print(hashmap* hm) {
  for (int i = 0; i < hm->capacity; i++) {
    char* key = hm_get_key(hm, i);
    if (memcmp(key, hm->zero_key, hm->key_size) != 0) {
      for (int j = 0; j < hm->key_size; j++) {
        printf("%02hhx", *(key + j));
        if (j != hm->key_size - 1) {
          printf(" ");
        }
      } 
      printf("/");
      for (int j = 0; j < hm->val_size; j++) {
        printf("%02hhx", *(key + hm->key_size + j));
        if (j != hm->val_size - 1) {
          printf(" ");
        }
      }
      printf("\n");
    }
  }
}

static void* hm_begin(hashmap* hm) {
  return hm->data;
}

static void* hm_end(hashmap* hm) {
  return hm->data + hm->capacity * (hm->key_size + hm->val_size);
}

static bool hm_exist(hashmap* hm, size_t index) {
  return memcmp(hm_get_key(hm, index), hm->zero_key, hm->key_size) != 0;
}
