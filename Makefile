CC=clang++
LIB=tickdb
LIBSO := lib$(LIB).so

BUILD_DIR ?= ./build
SRC_DIR ?= ./lib
LDFLAGS ?= -shared
CFLAGS += -std=gnu++17 -g -O3 -fPIC

SRCS := $(wildcard lib/*.cc)
OBJS := $(SRCS:%=$(BUILD_DIR)/obj/%.o)

$(BUILD_DIR)/$(LIBSO): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/obj/%.cc.o: %.cc
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)

-include $(OBJS:.o=.d)

.PHONY: examples
examples: $(BUILD_DIR)/$(LIBSO)
	make -C examples

.PHONY: test
test: examples
	./build/examples/trades

