LIB=tickdb
LIBSO := lib$(LIB).so

BUILD_DIR ?= ./build
SRC_DIR ?= ./src
LDFLAGS ?= -shared
CFLAGS += -std=gnu11 -g -fpic -fvisibility=hidden -O3

SRCS := $(wildcard $(SRC_DIR)/*.c $(SRC_DIR)/util/*.c)
OBJS := $(SRCS:%=$(BUILD_DIR)/obj/%.o)

$(BUILD_DIR)/$(LIBSO): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/obj/%.c.o: %.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)

-include $(OBJS:.o=.d)

.PHONY: examples
examples: $(BUILD_DIR)/$(LIBSO)
	make -C examples

.PHONY: tools
tools: $(BUILD_DIR)/$(LIBSO)
	make -C tools

.PHONY: test
test: examples
	$(RM) -rf data
	perf stat ./build/examples/trades

