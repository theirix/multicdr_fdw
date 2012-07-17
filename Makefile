MODULES = multicdr_fdw

EXTENSION = multicdr_fdw
DATA = multicdr_fdw--1.0.sql

REGRESS = multicdr_fdw
REGRESS_OPTS = --user postgres

EXTRA_CLEAN = sql/multicdr_fdw.sql expected/multicdr_fdw.out

#PG_CPPFLAGS += -Wno-format -g -O0
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
