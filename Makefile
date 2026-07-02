CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -Iinclude -O2
LDFLAGS = -pthread

SRC_DIR  = src
TEST_DIR = tests
OBJ_DIR  = build

SRCS = \
	$(SRC_DIR)/packet.c \
	$(SRC_DIR)/flow_buffer.c

OBJS = $(SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)
LIB  = $(OBJ_DIR)/libmulti_flow.a
TEST_BIN = $(OBJ_DIR)/run_tests

.PHONY: all test check sanitize clean

all: $(LIB)

test check: $(TEST_BIN)
	./$(TEST_BIN)

sanitize: CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address,undefined
sanitize: clean test

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(OBJS) | $(OBJ_DIR)
	ar rcs $@ $^

$(TEST_BIN): $(TEST_DIR)/test_flow_buffer.c $(OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(TEST_DIR)/test_flow_buffer.c $(OBJS) -o $@ $(LDFLAGS)

clean:
	rm -rf $(OBJ_DIR)
