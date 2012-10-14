MultiCDR FDW
===================

Foreign Data Wrapper for dealing with multiple CDR files.
CDR files from a directory can be read into a table with a 
specified field-to-column mapping.

Works with PostgreSQL 9.1.

Usage
-----

Please refer to a PostgreSQL Foreign Data Wrapper documentation.
In a few words extension, foreign data server and foreign table
must be created with a superuser permission. Then a user with 
appropriate permissions can select CDR data from a table.

CDR file consists of multiple rows. Each row contains data fields at
fixed positions. List of positions is specified when table is created.
Some special rows are ignored if they contains not enough columns.

Now only integer (int4) and text column types are allowed. 
Please check fields for overflow.

CDR search can be controlled with date restrictons.
Minimum and maximum dates can be specified in a query so extension
could early skip a file if it does not match predefined date pattern.

Options
-------

### directory
Text, mandatory.
A directory where FDW should look for files. Scan is not recursive.

### pattern
Text, mandatory.
Posix Extended regex for selecting a file path.
If dateformat is used, specify a date portion as a regex group.

### dateformat
Text, optional.
Specifies how date is stored in a filename. Uses a following format:
$filename_pattern_group_no=date_regex
Pattern must have a 'filename_pattern_group_no' group which contains a date
portion which must be parsed with a 'date_regex' regex.

### dateminfield, datemaxfield
Timestamp, optional.
Must be specified if a dateformat is specified.
Contains column names with a minimum or maximum date restriction.
For using date restrictions one should provide values for these fields
using a WHERE clause. Only '=' operator for timestamps is recognized.

### filefield
Text, optional.
Name of the column that should contain a row's file path.

### mapfields
Text array with a comma-separated integers, optional.
Specifies a mapping from table columns to a CDR fields. For example, "4,5,6"
says that fourth, fifth and sixth fields from each CDR row will be read to a
table. An array and a table must have equal dimensions.
Fields are mapped sequentially if mapfields is not specified.
Default mapping is a one-to-one.

### posfields
Text array with a comma-separated integers, mandatory.
Specified positions in chars where each CDR field is started. Fields are
always left-aligned so a position should point to the non-space.

### rowminlen, rowmaxlen
Integers (specified as text), optional.
Minimum and maximum lengths of valid CDR row.
Row must satisfy to specified length restriction to be fetched into database.

Installing extension
--------------------

### Building and installing from source

Assume you are a current postgres user.

1. Build an extension

        gmake PG_CONFIG=$POSTGRES_DIR/bin/pg_config clean all install

2. Restart a server
3. Create extension, foreign server and table 
4. Optionally, run tests

        gmake PG_CONFIG=$POSTGRES_DIR/bin/pg_config PGUSER=postgres installcheck

### Installing from prebuilt version

It is the easiest automatic way.

1. Invoke a command in a distributon directory:

        gmake PG_CONFIG=$POSTGRES_DIR/bin/pg_config install

2. Restart a server
3. Create extension, foreign server and table

### Installing from prebuilt version

Manual way.

1. Copy `multicdr_fdw.so` to the pkglibdir dir. Consult with `$POSTGRES_DIR/bin/pg_config --pkglibdir` 
2. Copy `multicdr_fdw.control` and `multicdr_fdw--<version>.sql` to extension dir. Consult with `$POSTGRES_DIR/bin/pg_config --sharedir`/extension
3. Restart a server
4. Create extension, foreign server and table

### Platform notes

Please note that path to the pg_config could be $POSTGRES_DIR/bin/64/pg_config for 64-bit solaris systems.


Example initialization sql script
---------------------------------

        -- Should be run by a superuser

        CREATE EXTENSION multicdr_fdw;

        DROP FOREIGN TABLE IF EXISTS multicdr_test_table;
        DROP SERVER IF EXISTS multicdr_fdw_server;

        CREATE SERVER multicdr_fdw_server FOREIGN DATA WRAPPER multicdr_fdw;

        CREATE FOREIGN TABLE multicdr_test_table (
            field1 text,
            field2 text,
            field3 text,
            field4 text,
            field5 text
        ) SERVER multicdr_fdw_server OPTIONS (
            directory '/opt/pool',
            pattern '.*\.cdr',
            posfields '0,6,15,40,50,71,92');


Version history
---------------

Please consult with the Changes for version history information

License information
-------------------

You can use any code from this project under the terms of [PostgreSQL License](http://www.postgresql.org/about/licence/).

Please consult with the COPYING for license information.
