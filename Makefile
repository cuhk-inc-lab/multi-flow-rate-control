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
SPEC_DIR = apps/spec_pipeline
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
SPEC_BIN = $(OBJ_DIR)/spec_pipeline

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
SPEC_TEST_INPUT  = $(OBJ_DIR)/spec_test_input.ts
SPEC_TEST_OUTPUT = $(OBJ_DIR)/spec_test_output.ts

CX_SPEC_IN0  = $(OBJ_DIR)/cx_spec_in0.ts
CX_SPEC_OUT0 = $(OBJ_DIR)/cx_spec_out0.ts
CX_SPEC_IN1  = $(OBJ_DIR)/cx_spec_in1.ts
CX_SPEC_OUT1 = $(OBJ_DIR)/cx_spec_out1.ts
CX_SPEC_LARGE_IN  = $(OBJ_DIR)/cx_spec_large_in.ts
CX_SPEC_LARGE_OUT = $(OBJ_DIR)/cx_spec_large_out.ts
CX_RELAY_IN0  = $(OBJ_DIR)/cx_relay_in0.ts
CX_RELAY_OUT0 = $(OBJ_DIR)/cx_relay_out0.ts
CX_RELAY_IN1  = $(OBJ_DIR)/cx_relay_in1.ts
CX_RELAY_OUT1 = $(OBJ_DIR)/cx_relay_out1.ts

.PHONY: all test check demo app spec app-test app-test-multi spec-test complex-test sanitize tsan clean

all: $(LIB)

demo: $(DEMO_BIN)

app: $(RELAY_BIN)

spec: $(SPEC_BIN)

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

spec-test: $(SPEC_BIN)
	dd if=/dev/urandom of=$(SPEC_TEST_INPUT) bs=188 count=20 status=none
	./$(SPEC_BIN) --no-pace $(SPEC_TEST_INPUT) $(SPEC_TEST_OUTPUT)
	cmp $(SPEC_TEST_INPUT) $(SPEC_TEST_OUTPUT)

complex-test: $(SPEC_BIN) $(RELAY_BIN) $(TEST_BIN)
	./$(TEST_BIN)
	@echo "== spec multi-flow: 50 pkts + 30 pkts with 127B tail =="
	dd if=/dev/urandom of=$(CX_SPEC_IN0) bs=188 count=50 status=none
	dd if=/dev/urandom of=$(CX_SPEC_IN1) bs=188 count=30 status=none
	dd if=/dev/urandom bs=1 count=127 status=none >> $(CX_SPEC_IN1)
	./$(SPEC_BIN) --no-pace --multi $(CX_SPEC_IN0) $(CX_SPEC_OUT0) \
		$(CX_SPEC_IN1) $(CX_SPEC_OUT1)
	cmp $(CX_SPEC_IN0) $(CX_SPEC_OUT0)
	cmp $(CX_SPEC_IN1) $(CX_SPEC_OUT1)
	@echo "== spec large single-flow: 200 transport packets =="
	dd if=/dev/urandom of=$(CX_SPEC_LARGE_IN) bs=188 count=200 status=none
	./$(SPEC_BIN) --no-pace $(CX_SPEC_LARGE_IN) $(CX_SPEC_LARGE_OUT)
	cmp $(CX_SPEC_LARGE_IN) $(CX_SPEC_LARGE_OUT)
	@echo "== spec paced smoke: 30 pkts, pacing on =="
	dd if=/dev/urandom of=$(CX_SPEC_LARGE_IN) bs=188 count=30 status=none
	./$(SPEC_BIN) $(CX_SPEC_LARGE_IN) $(CX_SPEC_LARGE_OUT)
	@test $$(wc -c < $(CX_SPEC_LARGE_IN)) -eq $$(wc -c < $(CX_SPEC_LARGE_OUT))
	@echo "== relay multi-flow: uneven encode blocks + tail =="
	dd if=/dev/urandom of=$(CX_RELAY_IN0) bs=752 count=8 status=none
	dd if=/dev/urandom of=$(CX_RELAY_IN1) bs=752 count=3 status=none
	dd if=/dev/urandom bs=1 count=400 status=none >> $(CX_RELAY_IN1)
	./$(RELAY_BIN) --no-pace --multi $(CX_RELAY_IN0) $(CX_RELAY_OUT0) \
		$(CX_RELAY_IN1) $(CX_RELAY_OUT1)
	cmp $(CX_RELAY_IN0) $(CX_RELAY_OUT0)
	cmp $(CX_RELAY_IN1) $(CX_RELAY_OUT1)
	@echo "complex data-flow tests passed"

sanitize: CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address,undefined
sanitize: clean test spec-test app-test

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

$(OBJ_DIR)/spec_main.o: $(SPEC_DIR)/main.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -c $< -o $@

$(SPEC_BIN): $(OBJ_DIR)/spec_main.o $(OBJ_DIR)/relay_file_ingest.o $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) $(OBJ_DIR)/spec_main.o $(OBJ_DIR)/relay_file_ingest.o $(LIB_OBJS) -o $@ $(LDFLAGS)

clean:
	rm -rf $(OBJ_DIR)
