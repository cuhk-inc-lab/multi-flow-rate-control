CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -Iinclude -O2
LDFLAGS = -pthread

SRC_DIR  = src
TEST_DIR = tests
APP_DIR  = apps/demo
OBJ_DIR  = build

LIB_SRCS = \
	$(SRC_DIR)/packet.c \
	$(SRC_DIR)/time_utils.c \
	$(SRC_DIR)/flow_buffer.c \
	$(SRC_DIR)/mixed_queue.c \
	$(SRC_DIR)/fd_sink.c \
	$(SRC_DIR)/flow_context.c \
	$(SRC_DIR)/flow_worker.c \
	$(SRC_DIR)/dispatcher.c \
	$(SRC_DIR)/flow_manager.c

LIB_OBJS = $(LIB_SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o)
LIB      = $(OBJ_DIR)/libmulti_flow.a
TEST_BIN = $(OBJ_DIR)/run_tests
DEMO_BIN = $(OBJ_DIR)/multi_flow_demo

.PHONY: all test check demo sanitize clean

all: $(LIB)

demo: $(DEMO_BIN)

test check: $(TEST_BIN)
	./$(TEST_BIN)

sanitize: CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address,undefined
sanitize: clean test

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(LIB_OBJS) | $(OBJ_DIR)
	ar rcs $@ $^

$(TEST_BIN): $(TEST_DIR)/run_tests.c $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(TEST_DIR)/run_tests.c $(LIB_OBJS) -o $@ $(LDFLAGS)

$(DEMO_BIN): $(APP_DIR)/main.c $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(APP_DIR)/main.c $(LIB_OBJS) -o $@ $(LDFLAGS)

clean:
	rm -rf $(OBJ_DIR)
