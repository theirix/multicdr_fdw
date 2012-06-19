DROP FOREIGN TABLE IF EXISTS multicdr_test_table;
DROP SERVER IF EXISTS multicdr_fdw_server;

CREATE SERVER multicdr_fdw_server FOREIGN DATA WRAPPER multicdr_fdw;

GRANT USAGE ON FOREIGN SERVER multicdr_fdw_server TO testuser;

-- CREATE USER MAPPING FOR testuser SERVER multicdr_fdw_server;

CREATE FOREIGN TABLE multicdr_test_table (
    field1 text,
    field2 text,
    field3 text,
    field4 text,
    field5 text,
    field6 text,
    field7 text,
    field8 text,
    field9 text,
    field10 text
) SERVER multicdr_fdw_server
OPTIONS (
	directory '/projects/concerteza/sqlmed/multicdr_fdw/data/set2', 
	pattern '*.cdr',
	mapfields '',
	encoding 'UTF-8');
