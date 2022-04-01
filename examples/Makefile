BUILD_DIR := ../build
LIB := tickdb

TEST_DIR := examples
TEST_SRCS := $(wildcard *.c)
TEST_BINS := $(TEST_SRCS:%=$(BUILD_DIR)/$(TEST_DIR)/%)
TEST_BINS := $(patsubst %.c, %, $(TEST_BINS))

$(TEST_DIR): $(TEST_BINS)

$(BUILD_DIR)/$(TEST_DIR)/%: $(TEST_SRCS)
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -l$(LIB) -L$(BUILD_DIR) -g $< -o $(BUILD_DIR)/$(TEST_DIR)/$(patsubst %.c,%,$<) -Wl,-rpath=$(abspath $(BUILD_DIR))

.PHONY: test
test: $(TEST_BINS)
	./$<
	
