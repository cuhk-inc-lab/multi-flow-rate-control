CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -Iinclude -O2
LDFLAGS = -pthread

CB_DIR   = ../buffer-management-module
CB_INC   = $(CB_DIR)/include
CB_SRC   = $(CB_DIR)/src/circular_buffer.c

SRC_DIR  = src
TEST_DIR = tests
DEMO_DIR = apps/demo
RELAY_DIR = apps/multi_flow_relay
OBJ_DIR  = build

CFLAGS += -I$(CB_INC)

LIB_SRCS = \
	$(SRC_DIR)/packet.c \
	$(SRC_DIR)/time_utils.c \
	$(SRC_DIR)/flow_buffer.c \
	$(SRC_DIR)/mixed_queue.c \
	$(SRC_DIR)/fd_sink.c \
	$(SRC_DIR)/flow_context.c \
	$(SRC_DIR)/flow_worker.c \
	$(SRC_DIR)/dispatcher.c \
	$(SRC_DIR)/flow_manager.c \
	$(SRC_DIR)/packet_framer.c \
	$(SRC_DIR)/pipe_io.c

LIB_OBJS = $(LIB_SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o) \
           $(OBJ_DIR)/circular_buffer.o

LIB      = $(OBJ_DIR)/libmulti_flow.a
TEST_BIN = $(OBJ_DIR)/run_tests
DEMO_BIN = $(OBJ_DIR)/multi_flow_demo
RELAY_BIN = $(OBJ_DIR)/multi_flow_relay

RELAY_SRCS = \
	$(RELAY_DIR)/main.c \
	$(RELAY_DIR)/relay.c \
	$(RELAY_DIR)/codec.c \
	$(RELAY_DIR)/block_codec.c \
	$(RELAY_DIR)/file_ingest.c \
	$(RELAY_DIR)/file_drain.c

RELAY_OBJS = \
	$(OBJ_DIR)/relay_main.o \
	$(OBJ_DIR)/relay_relay.o \
	$(OBJ_DIR)/relay_codec.o \
	$(OBJ_DIR)/relay_block_codec.o \
	$(OBJ_DIR)/relay_file_ingest.o \
	$(OBJ_DIR)/relay_file_drain.o

APP_TEST_INPUT  = $(OBJ_DIR)/app_test_input.ts
APP_TEST_OUTPUT = $(OBJ_DIR)/app_test_output.ts
APP_TEST_INPUT1 = $(OBJ_DIR)/app_test_input0.ts
APP_TEST_INPUT2 = $(OBJ_DIR)/app_test_input1.ts
APP_TEST_OUTPUT1 = $(OBJ_DIR)/app_test_output0.ts
APP_TEST_OUTPUT2 = $(OBJ_DIR)/app_test_output1.ts

.PHONY: all test check demo app app-test sanitize tsan clean

all: $(LIB)

demo: $(DEMO_BIN)

app: $(RELAY_BIN)

test check: $(TEST_BIN)
	./$(TEST_BIN)

app-test: $(RELAY_BIN)
	dd if=/dev/urandom of=$(APP_TEST_INPUT) bs=188 count=20 status=none
	dd if=/dev/urandom bs=1 count=96 status=none >> $(APP_TEST_INPUT)
	./$(RELAY_BIN) --no-pace $(APP_TEST_INPUT) $(APP_TEST_OUTPUT)
	cmp $(APP_TEST_INPUT) $(APP_TEST_OUTPUT)

app-test-multi: $(RELAY_BIN)
	dd if=/dev/urandom of=$(APP_TEST_INPUT1) bs=752 count=5 status=none
	dd if=/dev/urandom of=$(APP_TEST_INPUT2) bs=752 count=5 status=none
	./$(RELAY_BIN) --no-pace --multi $(APP_TEST_INPUT1) $(APP_TEST_OUTPUT1) \
		$(APP_TEST_INPUT2) $(APP_TEST_OUTPUT2)
	cmp $(APP_TEST_INPUT1) $(APP_TEST_OUTPUT1)
	cmp $(APP_TEST_INPUT2) $(APP_TEST_OUTPUT2)

sanitize: CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address,undefined
sanitize: clean test app-test

tsan: CFLAGS += -fsanitize=thread -fno-omit-frame-pointer
tsan: LDFLAGS += -fsanitize=thread
tsan: clean test app-test-multi

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR)/circular_buffer.o: $(CB_SRC) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(LIB_OBJS) | $(OBJ_DIR)
	ar rcs $@ $^

$(TEST_BIN): $(TEST_DIR)/run_tests.c $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(TEST_DIR)/run_tests.c $(LIB_OBJS) -o $@ $(LDFLAGS)

$(DEMO_BIN): $(DEMO_DIR)/main.c $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(DEMO_DIR)/main.c $(LIB_OBJS) -o $@ $(LDFLAGS)

$(OBJ_DIR)/relay_%.o: $(RELAY_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -c $< -o $@

$(RELAY_BIN): $(RELAY_OBJS) $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) $(RELAY_OBJS) $(LIB_OBJS) -o $@ $(LDFLAGS)

clean:
	rm -rf $(OBJ_DIR)
