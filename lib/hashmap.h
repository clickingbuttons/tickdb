#pragma once

#include "wyhash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOAD_FACTOR 0.7f

typedef struct hashmap_kv {
  char* key;
  void* val;
} hashmap_kv;

typedef struct hashmap {
  size_t key_size; // stride
  size_t val_size; // stride
  size_t capacity;
  size_t nmemb;
  hashmap_kv* data;
} hashmap;

#define hm_init(key_type, val_type) _hm_init(sizeof(key_type), sizeof(val_type))

static hashmap _hm_init(size_t key_size, size_t val_size) {
  const size_t default_capacity = 16;
  hashmap res = {
    .key_size = key_size,
    .val_size = val_size,
    .capacity = default_capacity,
    .nmemb = 0,
    .data = (hashmap_kv*)calloc(default_capacity, sizeof(hashmap_kv)),
  };
  return res;
}

static void hm_grow(hashmap* hm);

static void hm_put(hashmap* hm, char* key, void* val) {
  printf("hm_put %p %p\n", key, val);
  uint64_t index = wyhash(key, hm->key_size);

  // Fast modulus. We always grow by powers of 2.
  index = index & (hm->capacity - 1);

  if ((hm->nmemb + 1) > LOAD_FACTOR * hm->capacity) {
    hm_grow(hm);
  }

  if (hm->data[index].key == key) { // overwrite
    hm->data[index].val = val;
    printf("hm_put overwrite %lu %p\n", index, val);
  } else { // insert
    for (int i = 1; hm->data[index].key != 0; i++) {
      index += i * i;
      index = index & (hm->capacity - 1);
    }
    printf("hm_put insert %lu %p\n", index, val);
    hm->data[index].key = key;
    hm->data[index].val = val;
    hm->nmemb += 1;
  }
}

static void hm_grow(hashmap* hm) {
  size_t old_capacity = hm->capacity;
  hashmap_kv* old_data = hm->data;
  printf("grow %lu\n", hm->capacity);
  hm->capacity *= 2;
  hm->data = (hashmap_kv*)calloc(hm->capacity, sizeof(hashmap_kv));
  for (int i = 0; i < old_capacity; i++) {
    if (old_data[i].key == 0) {
      continue;
    }
    hashmap_kv kv = old_data[i];
    hm_put(hm, kv.key, kv.val);
  }
  free(old_data);
}

static void* hm_get(hashmap* hm, char* key) {
  size_t index = index & (hm->capacity - 1);
  for (int i = 1; hm->data[index].key != key; i++) {
    index += i * i;
    index = index & (hm->capacity - 1);
  }
  printf("get %lu\n", index);

  return hm->data[index].val;
}

static void hm_free(hashmap* hm) {
  free(hm);
}

static void hm_iter(hashmap* hm) {
  for (int i = 0; i < hm->capacity; i++) {
    printf("%p %p\n", hm->data[i].key, hm->data[i].val);
  }
}

