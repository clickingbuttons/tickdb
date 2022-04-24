BUILD_DIR := ../build
LIB := tickdb
CFLAGS += -g -O3

TOOLS_DIR := tools
TOOLS_SRCS := $(wildcard *.c)
TOOLS_BINS := $(TOOLS_SRCS:%=$(BUILD_DIR)/$(TOOLS_DIR)/%)
TOOLS_BINS := $(patsubst %.c, %, $(TOOLS_BINS))

$(TOOLS_DIR): $(TOOLS_BINS)

$(BUILD_DIR)/$(TOOLS_DIR)/%: $(TOOLS_SRCS)
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -l$(LIB) -L$(BUILD_DIR) $< -o $(BUILD_DIR)/$(TOOLS_DIR)/$(patsubst %.c,%,$<) -Wl,-rpath=$(abspath $(BUILD_DIR))

.PHONY: test
test: $(TOOLS_BINS)
	./$<
	
