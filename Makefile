EXTENSION    = multicdr_fdw
EXTVERSION   = 1.2.0
MODULE_big   = multicdr_fdw
OBJS         = $(patsubst %.c,%.o,$(wildcard src/*.c))
DATA         = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
#DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/input/*.source)
TESTSCLEAN   = $(patsubst test/input/%.source,test/sql/%.sql,$(TESTS)) \
							 $(patsubst test/input/%.source,expected/%.out,$(TESTS))
REGRESS      = $(patsubst test/input/%.source,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
PG_CONFIG    := pg_config
#PG_CPPFLAGS  = 
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql $(TESTSCLEAN)

foo: 
	echo $(TESTS)
	echo $(TESTSPARSED)
	echo $(REGRESS)

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
