DROP FOREIGN TABLE IF EXISTS multicdr_test_table;
DROP SERVER IF EXISTS multicdr_fdw_server;

CREATE SERVER multicdr_fdw_server FOREIGN DATA WRAPPER multicdr_fdw;

CREATE FOREIGN TABLE multicdr_test_table (
    field1 text,
    field2 text
) SERVER multicdr_fdw_server
OPTIONS (format 'text', delimiter ' ', 
	directory '/projects/concerteza/sqlmed/multicdr_fdw/data', 
	pattern '*.cdr',
	cdrfields '',
	null '');
