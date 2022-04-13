#pragma once

#include "inttypes.h"
#include "wyhash.h"

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HASHMAP_DEFAULT_CAPACITY 16
#define HASHMAP_LOAD_FACTOR 0.7f

template <typename K, typename V> class hashmap {
public:
  hashmap() {
    capacity = HASHMAP_DEFAULT_CAPACITY;
    data = (KV*)calloc(HASHMAP_DEFAULT_CAPACITY + 1, sizeof(KV)),
    empty_key = data + capacity;
  }
  ~hashmap() { free(data); }
  V* get(K* key) {
    u64 index = wyhash(key, sizeof(K)) & (capacity - 1);
    KV* offset = data + index;
    if (memcmp(offset, empty_key, sizeof(K)) == 0) {
      return NULL;
    }
    for (u64 i = 1; memcmp(offset, key, sizeof(K)) != 0; i++) {
      index += i * i;
      index = index & (capacity - 1);
      offset = data + index;
      if (memcmp(offset, empty_key, sizeof(K)) == 0) {
        return NULL;
      }
    }

    return &offset->value;
  }
  V* put(K* key, V* val) {
    u64 index = wyhash(key, sizeof(K)) & (capacity - 1);

    if (memcmp(empty_key, key, sizeof(K)) == 0) {
      fprintf(stderr, "cannot write empty key\n");
      exit(1);
    }

    if ((nmemb + 1) > HASHMAP_LOAD_FACTOR * capacity) {
      grow();
    }

    KV* existing = data + index;
    if (memcmp(existing, key, sizeof(K)) == 0) { // overwrite
      memcpy(existing + sizeof(K), val, sizeof(V));
      return &existing->value;
    } else { // insert
      KV* offset = data + index;
      for (int i = 1; memcmp(offset, empty_key, sizeof(K)) != 0; i++) {
        index += i * i;
        index = index & (capacity - 1);
        offset = data + index;
      }
      memcpy(offset, key, sizeof(K));
      memcpy(&offset->value, val, sizeof(V));
      nmemb += 1;
      return &offset->value;
    }
  }
  void grow() {
    size_t old_capacity = capacity;
    KV* old_data = data;
    capacity *= 2;
    data = (KV*)calloc(capacity + 1, sizeof(KV));
    empty_key = data + capacity;
    for (int i = 0; i < old_capacity; i++) {
      KV* item = old_data + i;
      if (memcmp(item, empty_key, sizeof(K)) == 0) {
        continue;
      }
      put(&item->key, &item->value);
    }
    free(old_data);
  }

private:
  size_t capacity;
  size_t nmemb;
  // TODO: try nopack
  struct KV {
    K key;
    V value;
  };
  KV* data;
  void* empty_key;
  void print() {
    for (int i = 0; i < capacity; i++) {
      KV* item = data + i;
      if (memcmp(&item->key, empty_key, sizeof(K)) != 0) {
        for (int j = 0; j < sizeof(K); j++) {
          printf("%02hhx", item->key);
          if (j != sizeof(K) - 1) {
            printf(" ");
          }
        }
        printf("/");
        for (int j = 0; j < sizeof(V); j++) {
          printf("%02hhx", item->value);
          if (j != sizeof(V) - 1) {
            printf(" ");
          }
        }
        printf("\n");
      }
    }
  }
};
