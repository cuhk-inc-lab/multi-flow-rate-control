CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -D_POSIX_C_SOURCE=200809L -Iinclude -O2
LDFLAGS = -pthread
RS_LDFLAGS = -lerasurecode_rs_vand

CB_DIR   = ../buffer-management-module
CB_INC   = $(CB_DIR)/include
CB_SRC   = $(CB_DIR)/src/circular_buffer.c

SRC_DIR  = src
TEST_DIR = tests
WG_DIR    = apps/wg_multi_pipeline
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
	$(SRC_DIR)/pipe_io.c \
	$(SRC_DIR)/flow_peer_map.c \
	$(SRC_DIR)/ingress_push.c

LIB_OBJS = $(LIB_SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o) \
           $(OBJ_DIR)/circular_buffer.o

LIB      = $(OBJ_DIR)/libmulti_flow.a
TEST_BIN = $(OBJ_DIR)/run_tests
WG_BIN    = $(OBJ_DIR)/wg_multi_pipeline
WG_CODEC_TEST_BIN = $(OBJ_DIR)/wg_codec_tests
FEC_TRACE_BIN = $(OBJ_DIR)/fec_trace

WG_APP_SRCS = \
	$(WG_DIR)/main.c \
	$(WG_DIR)/pipeline.c \
	$(WG_DIR)/buffer_transfer.c \
	$(WG_DIR)/codec.c \
	$(WG_DIR)/block_codec.c \
	$(WG_DIR)/copy_codec.c \
	$(WG_DIR)/xor_fec_codec.c \
	$(WG_DIR)/rs_fec_codec.c \
	$(WG_DIR)/wire_udp.c \
	$(WG_DIR)/file_drain.c

WG_OBJS = \
	$(OBJ_DIR)/wg_main.o \
	$(OBJ_DIR)/wg_pipeline.o \
	$(OBJ_DIR)/wg_buffer_transfer.o \
	$(OBJ_DIR)/wg_codec.o \
	$(OBJ_DIR)/wg_block_codec.o \
	$(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o \
	$(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_wire_udp.o \
	$(OBJ_DIR)/wg_file_drain.o

WG_TEST_IN0  = $(OBJ_DIR)/wg_test_in0.ts
WG_TEST_IN1  = $(OBJ_DIR)/wg_test_in1.ts
WG_TEST_IN2  = $(OBJ_DIR)/wg_test_in2.ts
WG_TEST_OUT0 = $(OBJ_DIR)/wg_test_out0.ts
WG_TEST_OUT1 = $(OBJ_DIR)/wg_test_out1.ts
WG_TEST_OUT2 = $(OBJ_DIR)/wg_test_out2.ts

.PHONY: all test check wg-demo integration-test fec-trace sanitize tsan clean

all: $(LIB)

wg-demo: $(WG_BIN)

fec-trace: $(FEC_TRACE_BIN)
	./$(FEC_TRACE_BIN)

test check: $(TEST_BIN)
	./$(TEST_BIN)

integration-test wg-demo-test: $(WG_BIN) $(WG_CODEC_TEST_BIN)
	./$(WG_CODEC_TEST_BIN)
	dd if=/dev/urandom of=$(WG_TEST_IN0) bs=188 count=20 status=none
	dd if=/dev/urandom of=$(WG_TEST_IN1) bs=752 count=5 status=none
	dd if=/dev/urandom of=$(WG_TEST_IN2) bs=188 count=40 status=none
	dd if=/dev/urandom bs=1 count=96 status=none >> $(WG_TEST_IN2)
	./$(WG_BIN) --no-pace --multi \
		$(WG_TEST_IN0) $(WG_TEST_OUT0) \
		$(WG_TEST_IN1) $(WG_TEST_OUT1) \
		$(WG_TEST_IN2) $(WG_TEST_OUT2)
	cmp $(WG_TEST_IN0) $(WG_TEST_OUT0)
	cmp $(WG_TEST_IN1) $(WG_TEST_OUT1)
	cmp $(WG_TEST_IN2) $(WG_TEST_OUT2)
	sh $(TEST_DIR)/wire_loopback_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_multi_flow_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_xor_fec_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_rs_fec_test.sh ./$(WG_BIN) $(OBJ_DIR)

sanitize: CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address,undefined
sanitize: clean test integration-test

tsan: CFLAGS += -fsanitize=thread -fno-omit-frame-pointer
tsan: LDFLAGS += -fsanitize=thread
tsan: clean test integration-test

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

INCLUDE_HDRS := $(wildcard include/*.h)
WG_HDRS := $(wildcard $(WG_DIR)/*.h)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c $(INCLUDE_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR)/circular_buffer.o: $(CB_SRC) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(LIB_OBJS) | $(OBJ_DIR)
	ar rcs $@ $^

$(TEST_BIN): $(TEST_DIR)/run_tests.c $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(TEST_DIR)/run_tests.c $(LIB_OBJS) -o $@ $(LDFLAGS)

$(OBJ_DIR)/wg_%.o: $(WG_DIR)/%.c $(INCLUDE_HDRS) $(WG_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) -c $< -o $@

$(WG_BIN): $(WG_OBJS) $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $(WG_OBJS) $(LIB_OBJS) -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(WG_CODEC_TEST_BIN): $(TEST_DIR)/wg_codec_tests.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(FEC_TRACE_BIN): $(TEST_DIR)/fec_trace.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o \
	$(OBJ_DIR)/wg_copy_codec.o $(OBJ_DIR)/wg_xor_fec_codec.o \
	$(OBJ_DIR)/wg_rs_fec_codec.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

clean:
	rm -rf $(OBJ_DIR)
