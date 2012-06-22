﻿DROP FOREIGN TABLE IF EXISTS multicdr_test_table;
DROP SERVER IF EXISTS multicdr_fdw_server;

CREATE SERVER multicdr_fdw_server FOREIGN DATA WRAPPER multicdr_fdw;

GRANT USAGE ON FOREIGN SERVER multicdr_fdw_server TO testuser;

-- CREATE USER MAPPING FOR testuser SERVER multicdr_fdw_server;

CREATE FOREIGN TABLE multicdr_test_table (
    field1 text,
    field2 integer,
    field3 text,
    field4 integer,
    field5 text,
    field6 text,
    field7 text,
    field8 text,
    field9 text,
    field10 integer
) SERVER multicdr_fdw_server
OPTIONS (
	directory '/projects/concerteza/sqlmed/multicdr_fdw/data/set1', 
	pattern '.*\.cdr$',
	posfields '0,6,15,40,50,71,92,113,175,212,217,227,232,319,329',
	mapfields '0,1,2,3,4,5,6,7,8,9');
