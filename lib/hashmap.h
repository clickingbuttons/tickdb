#pragma once

#include "wyhash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOAD_FACTOR 0.7f

typedef struct hashmap {
  size_t key_size; // stride
  size_t val_size; // stride
  size_t capacity;
  size_t nmemb;
  void* zero_key; // used for testing until add bits per-entry
  char* data;
} hashmap;

#define hm_init(key_type, val_type) _hm_init(sizeof(key_type), sizeof(val_type))

static hashmap _hm_init(size_t key_size, size_t val_size) {
  const size_t default_capacity = 16;
  hashmap res = {
    .key_size = key_size,
    .val_size = val_size,
    .capacity = default_capacity,
    .nmemb = 0,
    .zero_key = calloc(1, key_size),
    .data = (char*)calloc(default_capacity, key_size + val_size),
  };
  return res;
}

static void hm_grow(hashmap* hm);

#define hm_put(hm, key, val) { \
  typeof(key) key_copy = key;  \
  typeof(val) val_copy = val;  \
  _hm_put(hm, &key_copy, &val_copy);     \
}

static inline char* get_key_start(hashmap* hm, char* start, size_t index) {
  return start + index * (hm->key_size + hm->val_size);
}

static inline char* get_key(hashmap* hm, size_t index) {
  return get_key_start(hm, hm->data, index);
}

static inline char* find_key_at(hashmap* hm, void* key, size_t index) {
  char* res = get_key(hm, index);
  for (int i = 1; memcmp(res, key, hm->key_size) != 0; i++) {
    index += i * i;
    index = index & (hm->capacity - 1);
    res = get_key(hm, index);
  }

  return res;
}

static void _hm_put(hashmap* hm, void* key, void* val) {
  printf("hm_put %p %p\n", key, val);
  uint64_t index = wyhash(key, hm->key_size);
  index &= (hm->capacity - 1);

  if ((hm->nmemb + 1) > LOAD_FACTOR * hm->capacity) {
    hm_grow(hm);
  }

  char* existing_key = hm->data + index * (hm->key_size + hm->val_size);
  if (memcmp(existing_key, key, hm->key_size) == 0) { // overwrite
    memcpy(existing_key + hm->key_size, val, hm->val_size);
  } else { // insert
    char* offset = find_key_at(hm, hm->zero_key, index);
    memcpy(offset, key, hm->key_size);
    memcpy(offset + hm->key_size, val, hm->val_size);
    hm->nmemb += 1;
  }
}

static void hm_grow(hashmap* hm) {
  size_t old_capacity = hm->capacity;
  char* old_data = hm->data;
  printf("grow %lu\n", hm->capacity);
  hm->capacity *= 2;
  hm->data = (char*)calloc(hm->capacity, hm->key_size + hm->val_size);
  for (int i = 0; i < old_capacity; i++) {
    char* key = get_key_start(hm, old_data, i);
    if (memcmp(key, hm->zero_key, hm->key_size) == 0) {
      continue;
    }
    _hm_put(hm, key, key + hm->key_size);
  }
  free(old_data);
}

#define hm_get(hm, key) ({ \
  typeof(key) key_copy = key;  \
  _hm_get(hm, &key_copy);     \
})

static void* _hm_get(hashmap* hm, void* key) {
  uint64_t index = wyhash(key, hm->key_size);
  index &= (hm->capacity - 1);
  char* offset = find_key_at(hm, key, index);
  printf("get %lu\n", index);

  return offset + hm->key_size;
}

static void hm_free(hashmap* hm) {
  free(hm);
}

static void hm_iter(hashmap* hm) {
  for (int i = 0; i < hm->capacity; i++) {
    char* offset = get_key(hm, i);
    printf("%c %d\n", *((char*)offset), *((int*)(offset + hm->key_size)));
  }
}

