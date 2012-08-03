DROP FOREIGN TABLE IF EXISTS multicdr_test_table;
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
    field10 integer,

		filename text,
		datemin TIMESTAMP,
		datemax TIMESTAMP
) SERVER multicdr_fdw_server
OPTIONS (
	directory '/projects/concerteza/sqlmed/multicdr_fdw/data/regression3', 
	pattern 'MSC_(.*)-([[:digit:]]+)\.cdr$',
	dateformat '$1=YYYYMMDDHHMISS',
	posfields '0,6,15,40,50,71,92,113,175,212,217,227,232,319,329',
	mapfields '0,1,2,3,4,5,6,7,8,9, 0,0,0',
	dateminfield 'datemin',
	datemaxfield 'datemax',
	filefield 'filename'
);

-- select filename from multicdr_test_table where	datemin = cast('2009-12-01 01:23:45' as timestamp) and
--		datemax = cast('2012-12-01 01:23:45' as timestamp);
