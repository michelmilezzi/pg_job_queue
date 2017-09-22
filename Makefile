MODULES = job_queue

EXTENSION = job_queue
DATA = job_queue--1.0.sql
PGFILEDESC = "job_queue - simple async queue mechanism"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/job_queue
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
