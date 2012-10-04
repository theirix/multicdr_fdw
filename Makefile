MODULES = multicdr_fdw

EXTENSION = multicdr_fdw
DATA = multicdr_fdw--1.1.sql

REGRESS = multicdr_fdw

EXTRA_CLEAN = sql/multicdr_fdw.sql expected/multicdr_fdw.out

#PG_CPPFLAGS += -Wno-format -g -O0
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
