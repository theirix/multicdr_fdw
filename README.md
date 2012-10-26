MultiCDR FDW
===================

Foreign Data Wrapper for representing CDR files stream as an external SQL table. CDR files from a directory can be read into a table with a 
specified field-to-column mapping.

Works with PostgreSQL 9.1 and 9.2.

Usage
-----

Please consult with doc/multicdr_fdw.md for a function and operator reference.

On PGXN please click on extension from _Extensions_ section to view reference.

Installing extension
--------------------

To use an extension one must be built, installed into PostgreSQL directory
and registered in a database.

### Building extension

#### Method 1: Using PGXN network

The easisest method to get and install an extension from PGXN network.
PGXN client downloads and builds the extension.

    pgxn --pg_config <postgresql_install_dir>/bin/pg_config install multicdr_fdw

PGXN client itself is available at [github](https://github.com/dvarrazzo/pgxnclient) and
can be installed with your favourite method, i.e. `easy_install pgxnclient`.

#### Method 2: Using PGXS makefiles

C extension are best built and installed using [PGXS](http://www.postgresql.org/docs/9.1/static/extend-pgxs.html).
PGXS ensures that make is performed with needed compiler and flags. You only need GNU make and a compiler to build
an extension on an almost any UNIX platform (Linux, Solaris, OS X).

Compilation:

    gmake PG_CONFIG=<postgresql_install_dir>/bin/pg_config

Installation (as superuser):

    gmake PG_CONFIG=<postgresql_install_dir>/bin/pg_config install

PostgreSQL server must be restarted. 

To uninstall extension completely you may use this command (as superuser):

    gmake PG_CONFIG=<postgresql_install_dir>/bin/pg_config uninstall

Project contains SQL tests that can be launched on PostgreSQL with installed extension.
Tests are performed on a dynamically created database with a specified user (with the 
appropriated permissions - create database, for example):

    gmake PG_CONFIG=<postgresql_install_dir>/bin/pg_config PGUSER=postgres installcheck

#### Method 3: Manually

Use this method if you have a precompiled extension and do not want to install this with help of PGXS.
Or maybe you just do not have GNU make on a production server.
Or if you use Windows (use MSVC 2008 for Postgres 9.1 and MSVC 2010 for Postgres 9.2).

Copy library to the PostgreSQL library directory:

    cp multicdr_fdw.so `<postgresql_install_dir>/bin/pg_config --pkglibdir` 

Copy control file to the extension directory:
    
    cp multicdr_fdw.control `<postgresql_install_dir>/bin/pg_config --sharedir`/extension

Copy SQL prototypes file to the extension directory:
    
    cp multicdr_fdw--<version>.sql `<postgresql_install_dir>/bin/pg_config --sharedir`/extension

To uninstall extension just remove files you copied before.

### Platform notes

Please note that path to the pg_config could be $POSTGRES_DIR/bin/64/pg_config for 64-bit solaris systems.


### Creating extension in a database

Extension must be previously installed to a PostgreSQL directory.

Extension is created in a particular database (as superuser):

    create extension multicdr_fdw;

It creates all the functions, operators and other stuff from extension.
Note that you must restart a server if a previous library was already installed
at the same place. In other words, always restart to be sure. 

To drop an extension use:

    drop extension multicdr_fdw cascade;

Version history
---------------

Please consult with the Changes for version history information

License information
-------------------

You can use any code from this project under the terms of [PostgreSQL License](http://www.postgresql.org/about/licence/).

Please consult with the COPYING for license information.
