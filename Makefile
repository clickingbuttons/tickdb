CC=clang
TARGET_LIB ?= tickdb.so

BUILD_DIR ?= ./build
SRC_DIR ?= ./lib
LDFLAGS ?= -shared

SRCS := $(shell find $(SRC_DIR) -name *.c)
OBJS := $(SRCS:%=$(BUILD_DIR)/obj/%.o)
DEPS := $(OBJS:.o=.d)

CFLAGS += -std=c11 -MMD -MP

$(BUILD_DIR)/$(TARGET_LIB): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/obj/%.c.o: %.c
	$(MKDIR_P) $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)

-include $(DEPS)

MKDIR_P ?= mkdir -p
