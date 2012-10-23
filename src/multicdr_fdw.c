/*-------------------------------------------------------------------------
 *
 * multicdr_fdw.c
 *		foreign-data wrapper for multiple CDR files
 *
 * Copyright (c) 2012, Con Certeza
 * Author: irix <theirix@concerteza.ru>
 *
 * FDW wrapper is inspired by the file_fdw (postgres contrib module)
 * and file_fixed_length_fdw (https://github.com/adunstan)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "access/reloptions.h"
#include "access/tupdesc.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/memnodes.h"
#include "nodes/print.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "regex/regex.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/timestamp.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MultiCdrFdwOption
{
	const char *optname;
	Oid optcontext; /* Oid of catalog in which option may appear */
};

/*
 * Valid options for multicdr_fdw.
 * These options are based on the options for COPY FROM command.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * fileGetOptions(), which currently doesn't bother to look at user mappings.
 */
static struct MultiCdrFdwOption valid_options[] = {
	/* File options */
	{"directory", ForeignTableRelationId},
	{"pattern", ForeignTableRelationId},
	{"dateformat", ForeignTableRelationId},
	{"posfields", ForeignTableRelationId},
	{"mapfields", ForeignTableRelationId},
	{"filefield", ForeignTableRelationId},
	{"rowminlen", ForeignTableRelationId},
	{"rowmaxlen", ForeignTableRelationId},
	{"dateminfield", ForeignTableRelationId},
	{"datemaxfield", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct MultiCdrExecutionState
{
	/* parameters */
	char			*directory;						/* directory to read */
	regex_t		pattern_regex;				/* file path regex */
	int				regex_date_group_num;	/* group number with a pattern containg a date */
	char			*date_format;					/* date format */
	char			*file_field;					/* name of field to write a filename */
	char			*datemin_field;				/* name of field to write a min date */
	char			*datemax_field;				/* name of field to write a max date */
	int				rowminlen;						/* minimum length of valid cdr row */
	int				rowmaxlen;						/* maximum length of valid cdr row */

	/* computed parameters */
	Timestamp	datemin_timestamp;		/* timestamp with min date */
	Timestamp	datemax_timestamp;		/* timestamp with max date */
	int				file_field_column;		/* column number for a file_field */
	int				datemin_field_column;	/* column number for a datemin */
	int				datemax_field_column;	/* column number for a datemax */

	int				*pos_fields;					/* array for posfields option */
	int				pos_fields_count;			/* array size for posfields option */
	int				*map_fields;					/* array for mapfields option */
	int				map_fields_count;			/* array size for mapfields option */
	Oid				*column_types;				/* types of columns */
	List			*op_oids;							/* allowed operator oids */
	
	/* context */
	char	*read_buf;							/* null-terminated buffer for a whole CDR line */
	int		read_buf_size;					/* current read_buf size */
	int		recnum;									/* current record number */
	int		cdr_row;								/* row number of a current file */
	int		cdr_columns_count;			/* fields count in a normal CDR row */
	int		relation_columns_count;	/* relation size */

	char	**fields_start;					/* start pointer inside read_buf array for each CDR field */
	char	**fields_end;						/* end pointer inside read_buf array for each CDR field */

	/* file I/O */
	int		source;									/* current file descriptor */
	char	*file_buf;							/* file read buffer */
	char	*file_buf_start;				/* start pointer of actual data in file_buf */
	char	*file_buf_end;					/* end potiner of actual data in file_buf */

	List		*files;								/* list of all valid files */
	ListCell *current_file;				/* current file */

} MultiCdrExecutionState;

/* initial read_buf size */
#define MULTICDR_FDW_INITIAL_BUF_SIZE 128

/* initial file_buf size */
#define MULTICDR_FDW_FILEBUF_SIZE 512

/* log level, usually DEBUG5 (silent) or NOTICE (messages are sent to client side) */
#define MULTICDR_FDW_TRACE_LEVEL NOTICE

/*
 * SQL functions
 */
extern Datum multicdr_fdw_handler(PG_FUNCTION_ARGS);
extern Datum multicdr_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(multicdr_fdw_handler);
PG_FUNCTION_INFO_V1(multicdr_fdw_validator);

/*
 * FDW callback routines
 */

#if PG_VERSION_NUM >= 90200
static void fileGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void fileGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *fileGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses);
#else
static FdwPlan *filePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

static void fileExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void fileBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *fileIterateForeignScan(ForeignScanState *node);
static void fileReScanForeignScan(ForeignScanState *node);
static void fileEndForeignScan(ForeignScanState *node);


/*
 * Helper functions
 */
static void defGetStringOrNullPrecheck(DefElem* def, char** field);
static Timestamp parseTimestamp(const char *str, const char *fmt);
static bool is_valid_option(const char *option, Oid context);
static pg_wchar* make_wchar_dup(const char *tstr);
static char* regex_group_str(const char *str, const regmatch_t group);
static int find_column_by_name (const char *name, TupleDesc tuple_desc);
static void parseOptions(Oid catalog, List *options, MultiCdrExecutionState *state);
static void fileGetOptions(Oid foreigntableid, MultiCdrExecutionState *state);
static bool
fetchFileData(MultiCdrExecutionState *festate);
static bool
fetchLineFromFile(MultiCdrExecutionState *festate);
static bool
fetchLine(MultiCdrExecutionState *festate);
static bool
rewindToCdrLine(MultiCdrExecutionState *festate);
static bool 
makeTuple(MultiCdrExecutionState *festate, TupleTableSlot *slot);
static bool
parseLine(MultiCdrExecutionState *festate);
static int
parseIntArray(char *string, int **vals);
static long
safe_strtol (const char* text);
static bool
moveToNextFile(MultiCdrExecutionState *festate);


/* --------------------------------------------------------------- */

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
multicdr_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#if PG_VERSION_NUM >= 90200
	fdwroutine->AnalyzeForeignTable = NULL;

	fdwroutine->GetForeignRelSize = fileGetForeignRelSize;
	fdwroutine->GetForeignPaths = fileGetForeignPaths;
	fdwroutine->GetForeignPlan = fileGetForeignPlan;
#else
	fdwroutine->PlanForeignScan = filePlanForeignScan;
#endif

	fdwroutine->ExplainForeignScan = fileExplainForeignScan;
	fdwroutine->BeginForeignScan = fileBeginForeignScan;
	fdwroutine->IterateForeignScan = fileIterateForeignScan;
	fdwroutine->ReScanForeignScan = fileReScanForeignScan;
	fdwroutine->EndForeignScan = fileEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}


/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses multicdr_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
multicdr_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	MultiCdrExecutionState dummy_state;
	ListCell *cell;

	/*
		* Only superusers are allowed to set options of a multicdr_fdw foreign table.
		* This is because the filename is one of those options, and we don't want
		* non-superusers to be able to determine which file gets read.
		*
		* Putting this sort of permissions check in a validator is a bit of a
		* crock, but there doesn't seem to be any other place that can enforce
		* the check more cleanly.
		*
		* Note that the valid_options[] array disallows setting filename at any
		* options level other than foreign table --- otherwise there'd still be a
		* security hole.
		*/
	if (catalog == ForeignTableRelationId && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("only superuser can change options of a multicdr_fdw foreign table")));

	/* Check for unknown options */
  foreach(cell, options_list)
  {
    DefElem *def = (DefElem *) lfirst(cell);

    if (!is_valid_option(def->defname, catalog))
    {
      struct MultiCdrFdwOption *opt;
      StringInfoData buf;

      /*
        * Unknown option specified, complain about it. Provide a hint
        * with list of valid options for the object.
        */
      initStringInfo(&buf);
      for (opt = valid_options; opt->optname; opt++)
      {
        if (catalog == opt->optcontext)
          appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
            opt->optname);
      }

      ereport(ERROR,
          (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
            errmsg("invalid option \"%s\"", def->defname),
            errhint("valid options in this context are: %s", buf.data)));
    }
	}
	
	parseOptions(catalog, options_list, &dummy_state);

	PG_RETURN_VOID();
}

static pg_wchar*
make_wchar_dup(const char *tstr)
{
	pg_wchar *result;
	result = (pg_wchar*) palloc((strlen(tstr) + 1) * sizeof(pg_wchar));
	pg_mb2wchar(tstr, result);
	return result;
}

static char*
regex_group_str(const char *str, const regmatch_t group)
{
	char *result;
	result = palloc0(group.rm_eo - group.rm_so + 1);
	memcpy(result, str + group.rm_so, group.rm_eo - group.rm_so);
	return result;
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
	struct MultiCdrFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/* This syntax is required for checking for duplicate options
 * If options is specified twice, first 'if' evaluates
 */
static void
defGetStringOrNullPrecheck(DefElem* def, char **field)
{
	if (*field)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("conflicting or redundant options %s", def->defname)));
	*field = defGetString(def);
	if (!strlen(*field))
		*field = NULL;
}

static Timestamp
parseTimestamp(const char *str, const char *fmt)
{
  Datum timestamptz;
	Datum ts;
  timestamptz = DirectFunctionCall2(to_timestamp,
      PointerGetDatum(cstring_to_text(str)),
      PointerGetDatum(cstring_to_text(fmt)));
  Assert(timestamptz);
  ts = DirectFunctionCall1(timestamptz_timestamp,
      timestamptz);
  return DatumGetTimestamp(ts);
}

static const char*
timestamp_to_str(Timestamp timestamp)
{
  TimestampTz timestamptz = DatumGetTimestampTz(DirectFunctionCall1(timestamp_timestamptz, timestamp));
  return timestamptz_to_str(timestamptz);
}


static int
find_column_by_name (const char *name, TupleDesc tuple_desc)
{
	int i;
	if (name && strlen(name))
	{
		for (i = 0; i < tuple_desc->natts; ++i)
		{
			if (!strcmp(tuple_desc->attrs[i]->attname.data, name))
				return i;
		}
	}
	ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				errmsg("referenced column %s does not exist", name)));
	return -1;
}


static void
ereportRegexError(regex_t* regex, const char *prefix, int err)
{
	char *buf = NULL;
	size_t buf_len = 0;
	buf_len = pg_regerror(err, regex, buf, buf_len);
	buf = palloc(buf_len);
	buf_len = pg_regerror(err, regex, buf, buf_len);
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				errmsg("%s: invalid regular expression: %s", prefix, buf)));
}

static void
parseOptions(Oid catalog, List *options, MultiCdrExecutionState *state)
{
	ListCell *lc;
	int err;
	pg_wchar *wpattern;
	char *tpattern = NULL;
	char *date_format_str = NULL;
	char *map_fields_str = NULL;
	char *pos_fields_str = NULL;
	char *rowminlen = NULL, *rowmaxlen = NULL;
	pg_wchar* wdate_format_str;
	regex_t date_format_regex;
	regmatch_t groups[3];


	/* Read options */
	MemSet(state, 0, sizeof(MultiCdrExecutionState));

	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "directory") == 0)
		{
			defGetStringOrNullPrecheck(def, &state->directory);
		}
		else if (strcmp(def->defname, "pattern") == 0)
		{
			defGetStringOrNullPrecheck(def, &tpattern);
			wpattern = make_wchar_dup(tpattern ? tpattern : ".*");

			err = pg_regcomp(&state->pattern_regex, wpattern, pg_wchar_strlen(wpattern), 
					REG_EXTENDED,DEFAULT_COLLATION_OID);
			if (err)
				ereportRegexError(&state->pattern_regex, "pattern", err);
			pfree(wpattern);
		}
		else if (strcmp(def->defname, "dateformat") == 0)
		{
			// this complex block of code just splits 'dateformat' option to a group index and date format string
			//
			state->regex_date_group_num = -1;

			defGetStringOrNullPrecheck(def, &date_format_str);

			if (date_format_str == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("empty dateformat detected")));
			
			elog(MULTICDR_FDW_TRACE_LEVEL, "parsing date format string \"%s\"", date_format_str);

			wpattern = make_wchar_dup("\\$([[:digit:]]+)=(.*)");

			err = pg_regcomp(&date_format_regex, wpattern, pg_wchar_strlen(wpattern), 
					REG_EXTENDED,DEFAULT_COLLATION_OID);
			if (err)
				ereportRegexError(&date_format_regex, "dateformat", err);

			wdate_format_str = make_wchar_dup(date_format_str);
			err = pg_regexec(&date_format_regex, wdate_format_str, pg_wchar_strlen(wdate_format_str), 0, NULL, sizeof(groups)/sizeof(groups[0]), groups, 0);
			if (err)
				ereportRegexError(&state->pattern_regex, "dateformat parse", err);

			state->regex_date_group_num = safe_strtol(regex_group_str(date_format_str, groups[1]) );
			state->date_format = regex_group_str(date_format_str, groups[2]);

			pfree(wpattern);
			pg_regfree(&date_format_regex);
			
			elog(MULTICDR_FDW_TRACE_LEVEL, "using date format with group %d and format \"%s\"", state->regex_date_group_num, state->date_format);
		}
		else if (strcmp(def->defname, "filefield") == 0)
		{
			defGetStringOrNullPrecheck(def, &state->file_field);
		}
		else if (strcmp(def->defname, "rowminlen") == 0)
		{
			defGetStringOrNullPrecheck(def, &rowminlen);
			state->rowminlen = rowminlen ? safe_strtol(rowminlen) : 0;
		}
		else if (strcmp(def->defname, "rowmaxlen") == 0)
		{
			defGetStringOrNullPrecheck(def, &rowmaxlen);
			state->rowmaxlen = rowmaxlen ? safe_strtol(rowmaxlen) : 0;
		}
		else if (strcmp(def->defname, "dateminfield") == 0)
		{
			defGetStringOrNullPrecheck(def, &state->datemin_field);
		}
		else if (strcmp(def->defname, "datemaxfield") == 0)
		{
			defGetStringOrNullPrecheck(def, &state->datemax_field);
		}
		else if (strcmp(def->defname, "posfields") == 0)
		{
			defGetStringOrNullPrecheck(def, &pos_fields_str);
			state->pos_fields_count = parseIntArray(pos_fields_str, &state->pos_fields);
			if (state->pos_fields == NULL && state->pos_fields_count > 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("invalid position mapping")));
		}
		else if (strcmp(def->defname, "mapfields") == 0)
		{
			defGetStringOrNullPrecheck(def, &map_fields_str);
			state->map_fields_count = parseIntArray(map_fields_str, &state->map_fields);
			if (state->map_fields == NULL && state->map_fields_count > 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("invalid fields mapping")));
		}
	}

	if (catalog == ForeignTableRelationId)
	{
		if (state->pattern_regex.re_magic)
		{
			elog(MULTICDR_FDW_TRACE_LEVEL, "pattern has %lu groups", state->pattern_regex.re_nsub);
		}

		if (state->date_format)
		{
			if (state->pattern_regex.re_magic == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("date format references a missing pattern")));
			if (state->regex_date_group_num < 0 || state->regex_date_group_num > state->pattern_regex.re_nsub)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("date format references group %d in a pattern which has only %lu groups", 
									state->regex_date_group_num, state->pattern_regex.re_nsub)));
		}
			
		if (state->directory == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						errmsg("directory is required for foreign table")));
		}
		
		if ((state->date_format != NULL) != (state->datemin_field || state->datemax_field))
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("dateformat and dateminfield/datemaxfield must be specified simultaneously")));
		}

		if (state->rowminlen > 0 && state->rowmaxlen > 0 && state->rowminlen > state->rowmaxlen)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("rowminlen is greater than rowmaxlen")));
		}

	}
}

/*
 * Fetch the options for a multicdr_fdw foreign table.
 *
 * We have to separate out "filename" from the other options because
 * it must not appear in the options list passed to the core COPY code.
 */
static void
fileGetOptions(Oid foreigntableid, MultiCdrExecutionState *state)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List *options;

	/*
		* Extract options from FDW objects. We ignore user mappings because
		* multicdr_fdw doesn't have any options that can be specified there.
		*/
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	parseOptions(ForeignTableRelationId, options, state);
}


/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
fileExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	MultiCdrExecutionState state;

	/* Fetch options */
	fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &state);

	/*ExplainPropertyText("foreign Directory", state.directory, es);*/
}

static int
enumerateFiles (MultiCdrExecutionState *festate)
{
	DIR *dir;
	struct dirent *de;
	struct stat file_stat;
	char path[MAXPGPATH];
	const int buf_size = sizeof(path);
	int err;
	pg_wchar *wpath;
	bool should_pass = false;
	regmatch_t groups[16];
	char *file_date_str;
	Timestamp file_timestamp = (Timestamp)0;

	festate->files = NIL;
	festate->current_file = NULL;

	dir = opendir( festate->directory );

	if (!dir)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("cannot open directory \"%s\": %m", festate->directory)));
		return -1;
	}

	while ((de = readdir( dir )) != NULL)
	{
		strncpy( path, festate->directory, buf_size-1 );
		strncat( path, "/", buf_size-1 );
		strncat( path, de->d_name, buf_size-1 );
		
		if (stat( path, &file_stat ))
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("cannot retrieve file information for \"%s\"", path)));
			closedir( dir );
			return -1;
		}

		should_pass = true;
		/* check is it a file */
		if (should_pass)
		{
			if ((file_stat.st_mode & S_IFREG) == 0)
				should_pass = false;
		}

		/* check is filename is matched */
		if (should_pass)
		{
			wpath = make_wchar_dup(path);

			err = pg_regexec(&festate->pattern_regex, wpath, pg_wchar_strlen(wpath), 0, NULL, sizeof(groups)/sizeof(groups[0]), groups, 0);
			if (err)
			{
				elog(MULTICDR_FDW_TRACE_LEVEL, "skip unmatched file \"%s\"", path);
				should_pass = false;
			}
			pfree(wpath);
		}

		/* prepare check for time restriction */
		if (should_pass && festate->date_format)
		{
			/* extract date from filename by specified regex */
			if (festate->regex_date_group_num >= sizeof(groups)/sizeof(groups[0]))
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("regex group is out of range")));

			file_date_str = regex_group_str(path, groups[festate->regex_date_group_num]);

			elog(MULTICDR_FDW_TRACE_LEVEL, "date string is \"%s\", date format is \"%s\"", file_date_str, festate->date_format);

			PG_TRY();
			{
				file_timestamp = parseTimestamp(file_date_str, festate->date_format);
			}
			PG_CATCH();
			{
				if (geterrcode() == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
				{
					elog(MULTICDR_FDW_TRACE_LEVEL, "cannot parse date in a filename, skipping");
					should_pass = false;
				}
				else
				{
					PG_RE_THROW();
				}
			}
			PG_END_TRY();

			elog(MULTICDR_FDW_TRACE_LEVEL, "timestamp of a file \"%s\" is %s", path, timestamp_to_str(file_timestamp));
		}

		/* check datemin */
		if (should_pass && festate->date_format && festate->datemin_timestamp != DT_NOEND)
		{
			if (!DatumGetBool(DirectFunctionCall2(timestamp_ge, 
							TimestampGetDatum(file_timestamp),
							TimestampGetDatum(festate->datemin_timestamp)
					)))
			{
				elog(MULTICDR_FDW_TRACE_LEVEL, "skip by datemin %s", timestamp_to_str(festate->datemin_timestamp));
				should_pass = false;
			}
		}

		/* check datemax */
		if (should_pass && festate->date_format && festate->datemax_timestamp != DT_NOEND)
		{
			if (!DatumGetBool(DirectFunctionCall2(timestamp_le, 
							TimestampGetDatum(file_timestamp),
							TimestampGetDatum(festate->datemax_timestamp))))
			{
				elog(MULTICDR_FDW_TRACE_LEVEL, "skip by datemax %s", timestamp_to_str(festate->datemax_timestamp));
				should_pass = false;
			}
		}

		/* accept a file */
		if (should_pass)
		{
			elog(MULTICDR_FDW_TRACE_LEVEL, "this is a file you are looking for, move along \"%s\"", path);
			festate->files = lappend(festate->files, strdup(path));
		}
	}

	closedir( dir );
	return 0;
}

static void
fetch_valid_operators_oid(List** oids)
{
	int res;
	int i;
	bool isnull;
	Oid oid;
	MemoryContext	mctxt = CurrentMemoryContext, spimctxt;

	/* TODO make use of typcache.h */
	*oids = NIL;

	res	= SPI_connect();
	if(SPI_OK_CONNECT != res)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot spi connect: (%d)", res)));
	}
	res	= SPI_execute("select o.oid from pg_operator o, pg_type t where "\
				"(t.oid = o.oprleft or t.oid = o.oprright) and t.typname='timestamp' and o.oprname='=' and o.oprname!='+' and o.oprname!='-'",
			true, 0);
	if (res < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot execute SPI (%d)", res)));
	}

	for (i = 0; i < SPI_processed; ++i)
	{
		oid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
		spimctxt = MemoryContextSwitchTo(mctxt);
		*oids = lappend_oid(*oids, oid);
		MemoryContextSwitchTo(spimctxt);
	}
	res	= SPI_finish();
 	if(SPI_OK_FINISH != res)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot spi finish: (%d)", res)));
	}
}

/* returns true if an expression is deleted */
static bool
extract_date_expression(Node *node, TupleDesc tupdesc, const char* field_name, List *allowed_oids, Timestamp *timestamp)
{
	OpExpr *op = (OpExpr *) node;
	Node *left, *right, *tmp;
	Index varattno;
	char *key;
	char *s;

	if (!timestamp)
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("invalid pointer")));
	*timestamp = DT_NOEND;

	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		s = pretty_format_node_dump(nodeToString(node));
		elog(MULTICDR_FDW_TRACE_LEVEL, "node: %s", s );

		if (list_length(op->args) != 2)
		{
			elog(MULTICDR_FDW_TRACE_LEVEL, "strange dim %d", list_length(op->args));
			return false;
		}

		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		if(!(   (IsA(left, Var) && IsA(right, Const))
				||  (IsA(left, Const) && IsA(right, Var))
				))
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("one operand supposed to be column another constant")));
		if(IsA(left, Const) && IsA(right, Var))
		{
			tmp = left;
			left = right;
			right = tmp;
		}


		varattno = ((Var *) left)->varattno;
		if (varattno <= 0 || varattno > tupdesc->natts)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("wrong varattno index")));
		key = NameStr(tupdesc->attrs[varattno - 1]->attname);
		if (strcmp(key, field_name))
		{
			elog(MULTICDR_FDW_TRACE_LEVEL, "skipping expression for a node %s, needed %s", key, field_name);
			return false;
		}
		else
		{
			if (!list_member_oid(allowed_oids, op->opno))
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("operator opno=%d is not allowed", op->opno)));

			elog(MULTICDR_FDW_TRACE_LEVEL, "found node %s operation %d", key, op->opno);
			*timestamp = DatumGetTimestamp(((Const*)right)->constvalue);
			return true;
		}
	}

	return false;
}

/* returns true if an expression is deleted */
static bool
handle_condition (ExprState *state, ScanState *ss, const char* field_name, List *allowed_oids, Timestamp *timestamp)
{
	bool should_remove;
	Node *node;

	node = (Node*)state->expr;
	
	should_remove = extract_date_expression(node, ss->ss_currentRelation->rd_att, 
			field_name, allowed_oids, timestamp);
	if (should_remove)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "found condition for field %s: %s", 
				field_name, *timestamp != DT_NOEND ? timestamp_to_str(*timestamp) : "<none>");

		return true;
	}
	else
	{
		*timestamp = DT_NOEND;
	}
	return false;
}

static void
beginScan(MultiCdrExecutionState *festate, ForeignScanState *node)
{
	ListCell *cell;
	int i;
	TupleDesc td;
	ListCell *lc;
	List *removed;
	ExprState *state;
	Timestamp timestamp;

	td = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	festate->relation_columns_count = td->natts;

	/* save columns types */
	festate->column_types = palloc(festate->relation_columns_count * sizeof(Oid));
	for (i = 0; i < festate->relation_columns_count; ++i)
	{
		festate->column_types[i] = td->attrs[i]->atttypid;
		if (!(festate->column_types[i] == TEXTOID || 
					festate->column_types[i] == INT4OID || 
					(festate->column_types[i] == TIMESTAMPOID && (
								(festate->datemin_field && !strcmp(td->attrs[i]->attname.data, festate->datemin_field)) ||
								(festate->datemax_field && !strcmp(td->attrs[i]->attname.data, festate->datemax_field)))
				)))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("invalid column type %d, allowed integer or text only", festate->column_types[i])));
		}
	}

	/* find a column with filename and dates, -1 if not specified */
	festate->file_field_column = festate->file_field
		? find_column_by_name(festate->file_field, td) : -1;
	festate->datemin_field_column = festate->datemin_field
		? find_column_by_name(festate->datemin_field, td) : -1;
	festate->datemax_field_column = festate->datemax_field
		? find_column_by_name(festate->datemax_field, td) : -1;


	/* log columns */
	if (festate->file_field_column >= 0)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "use column %d for a filename", festate->file_field_column);
	}
	if (festate->datemin_field_column >= 0)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "using min date constraint: #%d %s",
			festate->datemin_field_column, festate->datemin_field ? festate->datemin_field : "none");
		if (festate->column_types[festate->datemin_field_column] != TIMESTAMPOID)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("invalid column type for date constraint, should be timestamp")));
	}
	if (festate->datemax_field_column >= 0)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "using max date constraint: #%d %s",
			festate->datemax_field_column, festate->datemax_field ? festate->datemax_field : "none");
		if (festate->column_types[festate->datemax_field_column] != TIMESTAMPOID)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("invalid column type for date constraint, should be timestamp")));
	}

	/* if mapping does not exists, create default mapping - one-to-one for existing fields */
	if (festate->map_fields_count == 0)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "using default one-to-one mapping for %d columns", festate->relation_columns_count);
		festate->map_fields_count = festate->relation_columns_count;
		festate->map_fields = palloc(festate->map_fields_count * sizeof(int));
		for (i = 0; i < festate->map_fields_count; ++i)
			festate->map_fields[i] = i;
	}

	/* extract date restrictions constants */
	festate->datemin_timestamp = festate->datemax_timestamp = DT_NOEND;
	if (node->ss.ps.plan->qual)
	{
		removed = NIL;
		foreach (lc, node->ss.ps.qual)
		{
			state = (ExprState*)lfirst(lc);
			if (festate->datemin_field && handle_condition(state, &node->ss, festate->datemin_field, 
						festate->op_oids, &timestamp))
			{
				removed = list_append_unique_ptr(removed, state);
				festate->datemin_timestamp = timestamp;
			}
			if (festate->datemax_field && handle_condition(state, &node->ss, festate->datemax_field, 
						festate->op_oids, &timestamp))
			{
				removed = list_append_unique_ptr(removed, state);
				festate->datemax_timestamp = timestamp;
			}
		}
		/* remove restriction clause from qual (implicitly-ANDed qual conditions) */
		/* don't remove nodes, it failes for a strange reason 
		foreach (lc, removed)
		{
			node->ss.ps.qual = list_delete(node->ss.ps.qual, lfirst(lc));
		}*/
	}


	/* state set up */

	/* set up memory buffers */
	festate->read_buf_size = MULTICDR_FDW_INITIAL_BUF_SIZE;
	festate->read_buf = palloc(festate->read_buf_size);
	festate->file_buf = palloc(MULTICDR_FDW_FILEBUF_SIZE);
	festate->file_buf_start = festate->file_buf_end = festate->file_buf;

	festate->cdr_columns_count = festate->pos_fields_count;
	festate->fields_start = palloc(festate->cdr_columns_count * sizeof(char*));
	festate->fields_end = palloc(festate->cdr_columns_count * sizeof(char*));
			
	/* verify that a mapping is feasible */
	for (i = 0; i < festate->map_fields_count; ++i)
	{
		if ((festate->map_fields[i] < 0 || festate->map_fields[i] >= festate->cdr_columns_count) &&
					!(i == festate->file_field_column || i == festate->datemin_field_column || i == festate->datemin_field_column))
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("cannot map field #%d to CDR field #%d", i, festate->map_fields[i])));
		}
	}

	/* fields mapping must agree to a relation dim */
	if (festate->map_fields_count != 0 && festate->map_fields_count != festate->relation_columns_count)
	{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						errmsg("mapping and relation dimensions must agree (%d vs %d)", festate->map_fields_count, festate->relation_columns_count)));
	}

	/* reset record counter */
	festate->recnum = 0;

	/* get list of files */
	enumerateFiles( festate );

	foreach (cell, festate->files)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "found file: \"%s\"", (char*)lfirst(cell));
	}

	/* set up first file */
	moveToNextFile(festate);
}

static bool
moveToNextFile(MultiCdrExecutionState *festate)
{
	bool is_first;

	is_first = festate->current_file == NULL;
	if (is_first)
		festate->current_file = list_head(festate->files);
	else
		festate->current_file = lnext(festate->current_file);

	if (festate->current_file == NULL)
		return false;
	
	elog(MULTICDR_FDW_TRACE_LEVEL, "current file: %s", (char*)lfirst(festate->current_file));

	if (!is_first)
	{
		Assert(festate->source > 0);
		if (festate->source > 0)
			close(festate->source);
	}

	/* open a file */
	festate->source = open( lfirst(festate->current_file), O_RDONLY);
	if (festate->source == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("unable to open file \"%s\": %m", (char*)lfirst(festate->current_file))));

	/* reset counters and file-specific data */
	festate->cdr_row = 0;

	return true;
}


static void
endScan(MultiCdrExecutionState *festate)
{
	elog(MULTICDR_FDW_TRACE_LEVEL, "endScan");
	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		if (festate->read_buf)
			pfree(festate->read_buf);
		if (festate->file_buf)
			pfree(festate->file_buf);
		if (festate->fields_start)
			pfree(festate->fields_start);
		if (festate->fields_end)
			pfree(festate->fields_end);
		if (festate->column_types)
			pfree(festate->column_types);

		festate->current_file = NULL;
		if (festate->files)
		{
			list_free(festate->files);
			festate->files = NIL;
		}
		if (festate->source > 0)
		{
			close(festate->source);
			festate->source = 0;
		}
	}
}

static void
freeOptions(MultiCdrExecutionState *festate)
{
	elog(MULTICDR_FDW_TRACE_LEVEL, "freeOptions");
		
	if (festate)
	{
		pg_regfree(&festate->pattern_regex);

		if (festate->map_fields)
			pfree(festate->map_fields);
		if (festate->pos_fields)
			pfree(festate->pos_fields);
	}
}

/*
 * fileBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
fileBeginForeignScan(ForeignScanState *node, int eflags)
{
	MultiCdrExecutionState *festate;

	/*
		* Do nothing in EXPLAIN (no ANALYZE) case. node->fdw_state stays NULL.
		*/
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	festate = (MultiCdrExecutionState *) palloc(sizeof(MultiCdrExecutionState));
	/* Fetch options of foreign table */
	fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation), festate);

	node->fdw_state = (void *) festate;

	/* find applicable operators */
	fetch_valid_operators_oid(&festate->op_oids);
	elog(MULTICDR_FDW_TRACE_LEVEL, "found %d applicable operators", list_length(festate->op_oids));
	/*foreach(op_oids_cell, op_oids)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "found op oid %d", (int)lfirst_oid(op_oids_cell) );
	}*/


	beginScan( festate, node );
}

static void
fileErrorCallback(void *arg)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState*) arg;
	char* fn;

	fn = festate->current_file ? lfirst(festate->current_file) : "<none>";
	errcontext("multiCDR Foreign Table filename: \"%s\" record: %d", fn, festate->recnum);
}

/*
 * fileIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
fileIterateForeignScan(ForeignScanState *node)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	bool found;
	ErrorContextCallback errcontext;

	/* Set up callback to identify error line number. */
	errcontext.callback = fileErrorCallback;
	errcontext.arg = (void *) festate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/*
		* The protocol for loading a virtual tuple into a slot is first
		* ExecClearTuple, then fill the values/isnull arrays, then
		* ExecStoreVirtualTuple. If we don't find another row in the file, we
		* just skip the last step, leaving the slot empty as required.
		*
		* We can pass ExprContext = NULL because we read all columns from the
		* file, so no need to evaluate default expressions.
		*
		* We can also pass tupleOid = NULL because we don't allow oids for
		* foreign tables.
		*/
	ExecClearTuple(slot);

	if (festate->current_file != NULL)
	{
		found = makeTuple(festate, slot);
		if (found)
		{
			ExecStoreVirtualTuple(slot);
			++festate->recnum;
		}
	}

	/* Remove error callback. */
	error_context_stack = errcontext.previous;

	return slot;
}

static bool
isCrLf (char c)
{
	return c == '\r' || c == '\n';
}

static char*
findEol (char *start, char *end)
{
	char *p;
	for (p = start; *p && p != end; ++p)
		if (isCrLf( *p ))
			return p;
	return NULL;
}

static bool
fetchFileData(MultiCdrExecutionState *festate)
{
	int nread;
	nread = read(festate->source, festate->file_buf, MULTICDR_FDW_FILEBUF_SIZE);
	if (nread == -1)
	{
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("cannot read from data file %s", (char*)lfirst(festate->current_file))));
		return false;
	}
	festate->file_buf_start = festate->file_buf;
	festate->file_buf_end = festate->file_buf + nread;
	return true;
}

/*
 * Read a whole line to the read_buf
 */
static bool
fetchLineFromFile(MultiCdrExecutionState *festate)
{
	int bytes_read = 0;
	int copy_amount;
	char *eol_pos;
	char *end_pos;
		
	/*elog(MULTICDR_FDW_TRACE_LEVEL, "start reading new record");*/

	/* initial filebuffer fetch */
	if (festate->file_buf_start == festate->file_buf_end)
	{
		if (!fetchFileData(festate))
			return false;
		if (festate->file_buf_end == festate->file_buf_start)
			return false;
	}

	/* skip all linefeeds at start of buffer */
	for (;;)
	{
		/* buffer is empty */
		if (festate->file_buf_start == festate->file_buf_end)
		{
			if (!fetchFileData(festate))
				return false;
		}
		if (isCrLf( *(festate->file_buf_start) ))
			++festate->file_buf_start;
		else
			break;
	}
	
	festate->read_buf[0] = 0;
	/* start reading */
	for (;;)
	{
		eol_pos = findEol( festate->file_buf_start, festate->file_buf_end );
		end_pos = eol_pos ? eol_pos : festate->file_buf_end;
		copy_amount = end_pos - festate->file_buf_start;

		/* realloc if needed */
		if (bytes_read + copy_amount >= festate->read_buf_size)
		{
			festate->read_buf_size = (bytes_read + copy_amount) * 1.4;
			elog(MULTICDR_FDW_TRACE_LEVEL, "file reader: realloc buffer to %d", festate->read_buf_size);
			festate->read_buf = repalloc( festate->read_buf, festate->read_buf_size );
		}
		memcpy( festate->read_buf + bytes_read, festate->file_buf_start, copy_amount );
		bytes_read += copy_amount;
		if (eol_pos)
		{
			/* search completed, save position for next search and exit */
			festate->file_buf_start = eol_pos;
			festate->read_buf[bytes_read] = 0;
			break;
		}
		else
		{
			if (festate->file_buf_end == festate->file_buf_start)
			{
				/*elog(MULTICDR_FDW_TRACE_LEVEL, "file reader: eof");*/
				return bytes_read > 0;
			}
			/* fetch new buffer and continue search */
			if (!fetchFileData(festate))
				return false;
		}
	}

	return bytes_read > 0;
}

/*
 * Fetches a next CDR line
 * Returns false if fatal read error happens or if all files have been read
 * Function handles file switching internally
 */
static bool
fetchLine(MultiCdrExecutionState *festate)
{
	if (fetchLineFromFile(festate))
	{
		++festate->cdr_row;
		return true;
	}

	elog(MULTICDR_FDW_TRACE_LEVEL, "fetch line failed, total read %d lines, move to next file", festate->cdr_row);
	/* eof */
	if (!moveToNextFile(festate))
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "all files scanned");
		return false;
	}
	/* retry reading */
	return fetchLineFromFile(festate);
}

/*
 * Parse comma-delimited integer array
 * Returns element count
 */
static int
parseIntArray(char *string, int **vals)
{
	char *start = string, *end;
	int result, count, initial_count = 32;

	if (!string || !*string)
	{
		*vals = NULL;
		return 0;
	}
	
	*vals = palloc(initial_count * sizeof(int));

	for (count = 0; ; ++count)
	{
		result = strtol(start,&end,10);
		if (end == start)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("cannot parse array")));
		(*vals)[count] = result;
		while (*end == ' ')
			++end;
		if (!*end)
				break;
		if (*end != ',')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("cannot parse array")));
		start = end + 1;
		if (count >= initial_count)
			*vals = repalloc(*vals, count * 1.4 * sizeof(int));
	}
	
	return count+1;
}

static long
safe_strtol (const char* text)
{
	long intval;
	char *endptr;
	intval = strtol(text, &endptr, 10);
	if (endptr != text + strlen(text))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					errmsg("wrong string treated as a number: %s", text)));
	}
	return intval;
}

/* 
 * Parse a line by field positions and store begin-end pairs 
 * Returns true if a line contains exact needed fields amount
 */
static bool
parseLine(MultiCdrExecutionState *festate)
{
	int cur_column;
	char *start, *end;
	size_t string_len;

	string_len = strlen(festate->read_buf);

	/* first of all check for a very large string */
	if (festate->pos_fields[festate->pos_fields_count-1] >= string_len)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "skipping special row %d because it does not fit into fields",
				festate->cdr_row);
		return false;
	}
	/* check for length restrictions */
	if ((festate->rowminlen > 0 && string_len < festate->rowminlen) ||
			(festate->rowmaxlen > 0 && string_len > festate->rowmaxlen))
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "skipping row %d with length %ld (must be [%d;%d])",
				festate->cdr_row, string_len, festate->rowminlen, festate->rowmaxlen);
		return false;
	}

	for (cur_column = 0; cur_column < festate->cdr_columns_count; ++cur_column)
	{
		start = festate->pos_fields[cur_column] + festate->read_buf;
		if (cur_column == festate->cdr_columns_count - 1)
			end = string_len + festate->read_buf;
		else
			end = festate->pos_fields[cur_column+1] + festate->read_buf;

		/* squeeze spaces */
		for (; start != end && *(end-1) == ' '; --end)
			;

		festate->fields_start[cur_column] = start;
		festate->fields_end[cur_column] = end;
	}
	
	return true;
}

/*
 * Rewind buffer to a good sized CDR row
 * Post-condition: read_buf contains good cdr line 
 * Returns true if ok, false if EOF
 */
static bool 
rewindToCdrLine(MultiCdrExecutionState *festate)
{
	do
	{
		if (!fetchLine(festate))
			return false;
	
	} while (!parseLine(festate));

	return true;
}

/*
 * Construct the text array from the read in data, and stash it in the slot 
 */ 
static bool 
makeTuple(MultiCdrExecutionState *festate, TupleTableSlot *slot)
{
	int column, cdr_field_mapped;
	char *start, *end, *conv, *temp;

	if (!rewindToCdrLine(festate))
		return false;

	for (column = 0; column < festate->relation_columns_count; ++column)
	{
		/* save a current filename if asked */
		if (column == festate->file_field_column)
		{
			slot->tts_isnull[column] = false;
			slot->tts_values[column] = PointerGetDatum(cstring_to_text(lfirst(festate->current_file)));
		} 
		else if (column == festate->datemin_field_column)
		{
			slot->tts_isnull[column] = false;
			slot->tts_values[column] = TimestampGetDatum(festate->datemin_timestamp);
		} 
		else if (column == festate->datemax_field_column)
		{
			slot->tts_isnull[column] = false;
			slot->tts_values[column] = TimestampGetDatum(festate->datemax_timestamp);
		}
		else
		{
			cdr_field_mapped = festate->map_fields[column];
			start = festate->fields_start[cdr_field_mapped];
			end = festate->fields_end[cdr_field_mapped];

			/*elog(MULTICDR_FDW_TRACE_LEVEL, "mapped column %d to field %d, %x:%x (%d)", column, cdr_field_mapped, start, end, end-start);*/

			/* check is a precaution, should never happens because if it's a good line there cannot be empty columns
			 * actually there can be empty column, NULL will be passed */
			if (start != end)
			{
				temp = pnstrdup(start, end-start);
				slot->tts_isnull[column] = false;
				switch (festate->column_types[column])
				{
					case INT4OID:
						slot->tts_values[column] = Int32GetDatum(safe_strtol(temp));
						break;

					case TEXTOID:
						conv = pg_any_to_server(temp, end-start, PG_LATIN1);
						slot->tts_values[column] = CStringGetTextDatum(conv);
						break;

					default:
						slot->tts_isnull[column] = true;
				}
				pfree(temp);
			}
			else
			{
				slot->tts_values[column] = 0;
				slot->tts_isnull[column] = true;
			}
		}
	}

	return true;
}


/*
 * fileEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
fileEndForeignScan(ForeignScanState *node)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState *) node->fdw_state;

	endScan( festate );
	freeOptions( festate );
}

/*
 * fileReScanForeignScan
 * Rescan table, possibly with new parameters
 */
static void
fileReScanForeignScan(ForeignScanState *node)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState *) node->fdw_state;
	
	elog(MULTICDR_FDW_TRACE_LEVEL, "rescan initiated");

	endScan( festate );
	beginScan( festate, node );
}

/* 
 * TODO make actual impl to use actual CDR file information
 */

#if PG_VERSION_NUM >= 90200
/*
 * PostgreSQL 9.2 plan functions are derived from file_fdw 
 */

/* 
 * Obtain relation size estimates for a foreign table 
 */
static void
fileGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	MultiCdrExecutionState *state;

	state = (MultiCdrExecutionState *) palloc(sizeof(MultiCdrExecutionState));
	/* Fetch options at the first time.
	 * It's a beginning of the planning. */
	fileGetOptions(foreigntableid, state);

	/* TODO do we need to save state here? */
	baserel->fdw_private = NIL;

	/* Estimate relation size */
	baserel->rows = 100;
}

/*
 * Create possible access paths for a scan on the foreign table
 */
static void
fileGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	/* because state is not saved to fdw_private
	 * MultiCdrExecutionState *state = (MultiCdrExecutionState*) baserel->fdw_private;*/
	Cost		startup_cost;
	Cost		total_cost;

	/* Estimate costs */
	startup_cost = baserel->baserestrictcost.startup;
	total_cost = startup_cost * 10;

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path*)create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NIL));		/* no fdw_private data */
}

/*
 * Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
fileGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses)
{
	Index scan_relid = baserel->relid;

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL);
}

#else
/* PostgreSQL 9.1 */

/*
 * filePlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
filePlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel)
{
	FdwPlan *fdwplan;
	MultiCdrExecutionState state;

	/* Fetch options */
	fileGetOptions(foreigntableid, &state);

	/* Construct FdwPlan with cost estimates */
	fdwplan = makeNode(FdwPlan);
	fdwplan->startup_cost = baserel->baserestrictcost.startup;
	fdwplan->total_cost = fdwplan->startup_cost * 10;
	fdwplan->fdw_private = NIL; /* not used */

	return fdwplan;
}
#endif

/* vim: set noexpandtab tabstop=4 shiftwidth=4: */
