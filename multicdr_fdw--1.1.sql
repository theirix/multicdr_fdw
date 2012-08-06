-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION multicdr_fdw" to load this file. \quit

CREATE FUNCTION multicdr_fdw_handler()
	RETURNS fdw_handler
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT;

CREATE FUNCTION multicdr_fdw_validator(text[], oid)
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER multicdr_fdw
  HANDLER multicdr_fdw_handler
  VALIDATOR multicdr_fdw_validator;
