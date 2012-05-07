MODULES = multicdr_fdw

EXTENSION = multicdr_fdw
DATA = multicdr_fdw--1.0.sql

REGRESS = multicdr_fdw
#REGRESS_OPTS = --user dba

#EXTRA_CLEAN = sql/multicdr_fdw.sql expected/multicdr_fdw.out

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
