MultiCDR FDW extension
======================

Installing
----------

    CREATE EXTENSION multicdr_fdw;

Extension is compatible witgh PostgreSQL 9.1 and 9.2. 

Description
-----------

Extension `multicdr_fdw` is a Foreign Data Wrapper for representing CDR files stream as an external SQL table. CDR files from a directory can be read into a table with a 
specified field-to-column mapping.

Usage
-----

Please refer to a PostgreSQL Foreign Data Wrapper documentation about 
[foreign data](http://www.postgresql.org/docs/9.2/static/ddl-foreign-data.html)

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


Interface
---------

### Foreign data table options

#### `directory`
Text, mandatory.
A directory where FDW should look for files. Scan is not recursive.

#### pattern
Text, mandatory.
Posix Extended regex for selecting a file path.
If dateformat is used, specify a date portion as a regex group.

#### `dateformat`
Text, optional.
Specifies how date is stored in a filename. Uses a following format:
$filename_pattern_group_no=date_regex
Pattern must have a 'filename_pattern_group_no' group which contains a date
portion which must be parsed with a 'date_regex' regex.

#### `dateminfield`, `datemaxfield`
Timestamp, optional.
Must be specified if a dateformat is specified.
Contains column names with a minimum or maximum date restriction.
For using date restrictions one should provide values for these fields
using a WHERE clause. Only '=' operator for timestamps is recognized.

#### `filefield`
Text, optional.
Name of the column that should contain a row's file path.

#### `mapfields`
Text array with a comma-separated integers, optional.
Specifies a mapping from table columns to a CDR fields. For example, "4,5,6"
says that fourth, fifth and sixth fields from each CDR row will be read to a
table. An array and a table must have equal dimensions.
Fields are mapped sequentially if mapfields is not specified.
Default mapping is a one-to-one.

#### `posfields`
Text array with a comma-separated integers, mandatory.
Specified positions in chars where each CDR field is started. Fields are
always left-aligned so a position should point to the non-space.

#### `rowminlen`, `rowmaxlen`
Integers (specified as text), optional.
Minimum and maximum lengths of valid CDR row.
Row must satisfy to specified length restriction to be fetched into database.

Example
-------

To use a foreign table a foreign server must be created.
Optionally user mapping, foreign server options should be specified.

Please consult a simple example (should be run by a superuser):

        -- Create an extension
        CREATE EXTENSION multicdr_fdw;

        DROP FOREIGN TABLE IF EXISTS multicdr_test_table;
        DROP SERVER IF EXISTS multicdr_fdw_server;

        -- Create a foreign server
        CREATE SERVER multicdr_fdw_server FOREIGN DATA WRAPPER multicdr_fdw;

        -- Create a foreign table
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



Author
------

Copyright (c) 2012, Con Certeza LLC. All Right Reserved.

Developed by [Eugene Seliverstov](theirix@concerteza.ru)

Copyright and License
---------------------

You can use any code from this project under the terms of [PostgreSQL License](http://www.postgresql.org/about/licence/).

Please consult with the COPYING for license information.
