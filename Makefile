BIN = longmynd
SRC = main.c nim.c ftdi.c stv0910.c stv0910_utils.c stvvglna.c stvvglna_utils.c stv6120.c stv6120_utils.c ftdi_usb.c fifo.c udp.c beep.c ts.c
SRC += web/web.c web/json.c
OBJ = ${SRC:.c=.o}

ifndef CC
CC = gcc
endif

# Build parallel
#MAKEFLAGS += -j$(shell nproc || printf 1)

COPT = -O3 -march=native -mtune=native
# Help detection for ARM SBCs, using devicetree
F_CHECKDTMODEL = $(if $(findstring $(1),$(shell cat /sys/firmware/devicetree/base/model 2>/dev/null)),$(2))
# Jetson Nano is detected correctly
# Raspberry Pi 1 / Zero is detected correctly
DTMODEL_RPI2 = Raspberry Pi 2 Model B
DTMODEL_RPI3 = Raspberry Pi 3 Model B
DTMODEL_RPI4 = Raspberry Pi 4 Model B
COPT_RPI2 = -mfpu=neon-vfpv4
COPT_RPI34 = -mfpu=neon-fp-armv8
COPT += $(call F_CHECKDTMODEL,$(DTMODEL_RPI2),$(COPT_RPI2))
COPT += $(call F_CHECKDTMODEL,$(DTMODEL_RPI3),$(COPT_RPI34))
COPT += $(call F_CHECKDTMODEL,$(DTMODEL_RPI4),$(COPT_RPI34))
# -funsafe-math-optimizations required for NEON, warning: may lead to loss of floating-point precision
COPT += -funsafe-math-optimizations

CFLAGS += -Wall -Wextra -Wpedantic -Wunused -DVERSION=\"${VER}\" -pthread -D_GNU_SOURCE
LDFLAGS += -lusb-1.0 -lm -lasound
LDFLAGS += -Wl,-Bstatic -lwebsockets -Wl,-Bdynamic

LWS_DIR = ./web/libwebsockets/
LWS_LIBSDIR = ${LWS_DIR}/build/include
LWS_OBJDIR = ${LWS_DIR}/build/lib

all: check-gitsubmodules check-lws ${BIN} fake_read

debug: COPT = -Og
debug: CFLAGS += -ggdb -fno-omit-frame-pointer
debug: all

werror: CFLAGS += -Werror
werror: all

fake_read:
	@echo "  CC     "$@
	@${CC} fake_read.c -o $@

$(BIN): ${OBJ}
	@echo "  LD     "$@
	@${CC} ${COPT} ${CFLAGS} -o $@ ${OBJ} -L $(LWS_OBJDIR) ${LDFLAGS}

%.o: %.c
	@echo "  CC     "$<
	@${CC} ${COPT} ${CFLAGS} -I $(LWS_LIBSDIR) -c -fPIC -o $@ $<

clean:
	@rm -rf ${BIN} fake_read ${OBJ}

tags:
	@ctags *

check-gitsubmodules:
	@if git submodule status | egrep -q '^[-]|^[+]' ; then \
		echo "INFO: Need to [re]initialize git submodules"; \
		git submodule update --init; \
	fi

check-lws:
	@if [ ! -f "${LWS_OBJDIR}/libwebsockets.a" ]; then \
		echo "INFO: Need to compile libwebsockets"; \
		mkdir -p ${LWS_DIR}/build/; \
		cd ${LWS_DIR}/build/; \
		cmake ../ -DLWS_WITH_SSL=off \
					-DLWS_WITH_SHARED=off \
					-DLWS_WITHOUT_CLIENT=on \
					-DLWS_WITHOUT_TESTAPPS=on; \
		make; \
	fi

.PHONY: all clean check-gitsubmodules check-lws
