#pragma once

#include "inttypes.h"
#include "wyhash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HASHMAP_DEFAULT_CAPACITY 16
#define HASHMAP_LOAD_FACTOR 0.7f

template <typename K, typename V>
class hashmap {
public:
	hashmap() {
		this->capacity = HASHMAP_DEFAULT_CAPACITY;
		this->data = (char*)calloc(HASHMAP_DEFAULT_CAPACITY + 1, sizeof(K) + sizeof(V)),
		this->empty_key = get_key(this->capacity);
	}
	~hashmap() {
		free(this->data);
	}
	V* get(K* key) {
		u64 index = wyhash(key, this->key_size) & (this->capacity - 1);
		char* offset = get_key(index);
		if (memcmp(offset, this->empty_key, this->key_size) == 0) {
			return NULL;
		}
		for (u64 i = 1; memcmp(offset, key, this->key_size) != 0; i++) {
			index += i * i;
			index = index & (this->capacity - 1);
			offset = get_key(index);
			if (memcmp(offset, this->empty_key, this->key_size) == 0) {
				return NULL;
			}
		}

		return (V*)(offset + this->key_size);
	}
	V* put(K* key, V* val) {
		u64 index = wyhash(key, this->key_size) & (this->capacity - 1);

		if (memcmp(this->empty_key, key, this->key_size) == 0) {
			fprintf(stderr, "cannot write 0 key\n");
			exit(1);
		}

		if ((this->nmemb + 1) > HASHMAP_LOAD_FACTOR * this->capacity) {
			this->grow();
		}

		char* existing_key = this->data + index * (this->key_size + this->val_size);
		if (memcmp(existing_key, key, this->key_size) == 0) { // overwrite
			memcpy(existing_key + this->key_size, val, this->val_size);
			return (V*)(existing_key + this->key_size);
		} else { // insert
			char* offset = this->get_key(index);
			for (int i = 1; memcmp(offset, this->empty_key, this->key_size) != 0; i++) {
				index += i * i;
				index = index & (this->capacity - 1);
				offset = get_key(index);
			}
			memcpy(offset, key, this->key_size);
			memcpy(offset + this->key_size, val, this->val_size);
			this->nmemb += 1;
			return (V*)(offset + this->key_size);
		}
	}
	void grow() {
		size_t old_capacity = this->capacity;
		char* old_data = this->data;
		this->capacity *= 2;
		this->data = (char*)calloc(this->capacity + 1, this->key_size + this->val_size);
		this->empty_key = get_key(this->capacity);
		for (int i = 0; i < old_capacity; i++) {
			K* key = (K*)get_key_at(old_data, i);
			if (memcmp(key, this->empty_key, this->key_size) == 0) {
				continue;
			}
			put(key, (V*)(key + this->key_size));
		}
		free(old_data);
	}
private:
  size_t key_size;
  size_t val_size;
  size_t capacity;
  size_t nmemb;
  char* data;
  void* empty_key;
	char* get_key_at(char* start, size_t index) {
		return start + index * (this->key_size + this->val_size);
	}
	char* get_key(size_t index) {
		return get_key_at(this->data, index);
	}
	void print() {
		for (int i = 0; i < this->capacity; i++) {
			char* key = hm_get_key(this, i);
			if (memcmp(key, this->empty_key, this->key_size) != 0) {
				for (int j = 0; j < this->key_size; j++) {
					printf("%02hhx", *(key + j));
					if (j != this->key_size - 1) {
						printf(" ");
					}
				}
				printf("/");
				for (int j = 0; j < this->val_size; j++) {
					printf("%02hhx", *(key + this->key_size + j));
					if (j != this->val_size - 1) {
						printf(" ");
					}
				}
				printf("\n");
			}
		}
	}
};

