/*-------------------------------------------------------------------------
 *
 * multicdr_fdw.c
 *    foreign-data wrapper for multiple CDR files
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
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "mb/pg_wchar.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "nodes/memnodes.h"


PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MultiCdrFdwOption
{
	const char *optname;
	Oid		optcontext;		/* Oid of catalog in which option may appear */
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
	{"mapfields", ForeignTableRelationId},
	{"filefield", ForeignTableRelationId},
	{"minfields", ForeignTableRelationId},

	/* it's like COPY's encoding */
	{"encoding", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct MultiCdrExecutionState
{
	/* parameters */
	char		*directory;		/* directory to read */
	regex_t	pattern_regex;
	char		*file_field;
	int			file_field_column;
	int			encoding;
	int			*map_fields;
	int			map_fields_count;
	int			min_fields;
	
	/* context */
	char	*read_buf;
	int		read_buf_size;
	int		recnum;
	int		cdr_row;
	int		cdr_columns_count;
	int		relation_columns_count;

	char	**fields_start;	
	char	**fields_end;		

	/* file I/O */
	int		source;
	char	*file_buf;
	char	*file_buf_start;
	char	*file_buf_end;

	List		*files;
	ListCell *current_file;
} MultiCdrExecutionState;

#define MULTICDR_FDW_INITIAL_BUF_SIZE 128
#define MULTICDR_FDW_FILEBUF_SIZE 512

#define MULTICDR_FDW_OPEN_FLAGS O_RDONLY 

#define MULTICDR_FDW_TRACE_LEVEL DEBUG5

#ifdef WIN32
#define multicdr_open _open
#else
#define multicdr_open open
#endif

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
static FdwPlan *filePlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel);
static void fileExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void fileBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *fileIterateForeignScan(ForeignScanState *node);
static void fileReScanForeignScan(ForeignScanState *node);
static void fileEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void fileGetOptions(Oid foreigntableid, MultiCdrExecutionState *state);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
				MultiCdrExecutionState *state,
				Cost *startup_cost, Cost *total_cost);
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
static int
parseLine(char* read_buf, char **fields_start, char **fields_end, int max_fields);
static int
parseIntArray(char *string, int **vals);
static bool
moveToNextFile(MultiCdrExecutionState *festate);


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
multicdr_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->PlanForeignScan = filePlanForeignScan;
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
	List    *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid		catalog = PG_GETARG_OID(1);
	char *directory = NULL, *pattern = NULL, *file_field = NULL, *map_fields_str = NULL, *min_fields_str = NULL,
				*encoding = NULL;
	int *map_fields;
	int map_fields_count;
	int min_fields;
	ListCell   *cell;

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

	/*
	 * Check that only options supported by multicdr_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

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
			    	errhint("Valid options in this context are: %s",
					    buf.data)));
		}

		/* Separate out own options, since ProcessCopyOptions won't allow it */
		if (strcmp(def->defname, "directory") == 0)
		{
			if (directory)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("conflicting or redundant options")));
			directory = defGetString(def);
		}
		else if (strcmp(def->defname, "pattern") == 0)
		{
			if (pattern)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("conflicting or redundant options")));
			pattern = defGetString(def);
		}
		else if (strcmp(def->defname, "filefield") == 0)
		{
			if (file_field)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("conflicting or redundant options")));
			file_field = defGetString(def);
		}
		else if (strcmp(def->defname, "mapfields") == 0)
		{
			if (map_fields_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("conflicting or redundant options")));
			map_fields_str = defGetString(def);
			map_fields_count = parseIntArray(map_fields_str, &map_fields);
			if (map_fields == NULL && map_fields_count != 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("invalid map fields")));
			if (map_fields)
				pfree(map_fields);
		}
		else if (strcmp(def->defname, "minfields") == 0)
		{
			if (min_fields_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("conflicting or redundant options")));
			min_fields_str = defGetString(def);
			min_fields = strtol(min_fields_str, NULL, 10);
			if (min_fields <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("invalid minimum CDR fields number '%s'", min_fields)));
		}
		else if (strcmp(def->defname, "encoding") == 0)
		{
			if (encoding)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("conflicting or redundant options")));
			encoding = defGetString(def);
			if (pg_char_to_encoding(encoding) == -1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				    	errmsg("invalid encoding name '%s'", encoding)));
		}
	}
	
	/*
	 * Options that are required for multicdr_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId)
	{
		if (directory == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    	errmsg("directory is required for multicdr_fdw foreign tables")));
		if (pattern == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    	errmsg("pattern is required for multicdr_fdw foreign tables")));
		if (map_fields_str == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    	errmsg("mapfields is required for multicdr_fdw foreign tables")));
		if (min_fields_str == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    	errmsg("mapfields is required for multicdr_fdw foreign tables")));
		if (encoding == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    	errmsg("encoding is required for multicdr_fdw foreign tables")));
	}

	PG_RETURN_VOID();
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

static void
ereportRegexError(regex_t* regex, int err)
{
	char *buf = NULL;
	size_t buf_len = 0;
	buf_len = pg_regerror(err, regex, buf, buf_len);
	buf = palloc(buf_len);
	buf_len = pg_regerror(err, regex, buf, buf_len);
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
			 errmsg("invalid regular expression: %s", buf)));
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
	List    *options;
	ListCell   *lc;

	char *tpattern;
	pg_wchar *wpattern;
	int err;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * multicdr_fdw doesn't have any options that can be specified there.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	state->files = NIL;

	/*
	 * Read options
	 */
	state->directory = state->file_field = NULL;
	state->map_fields = NULL;
	state->encoding = state->map_fields_count = 0;
	state->min_fields = 0;
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "directory") == 0)
		{
			state->directory = defGetString(def);
		}
		else if (strcmp(def->defname, "pattern") == 0)
		{
			tpattern = defGetString(def);

			wpattern = (pg_wchar*) palloc((strlen(tpattern) + 1) * sizeof(pg_wchar));
			pg_mb2wchar(tpattern, wpattern);

			err = pg_regcomp(&state->pattern_regex, wpattern, pg_wchar_strlen(wpattern), 
					REG_NOSUB|REG_EXTENDED,DEFAULT_COLLATION_OID);
			if (err)
				ereportRegexError(&state->pattern_regex, err);
			pfree(wpattern);
		}
		else if (strcmp(def->defname, "filefield") == 0)
		{
			state->file_field = defGetString(def);
			if (!strlen(state->file_field))
				state->file_field = NULL;
		}
		else if (strcmp(def->defname, "mapfields") == 0)
		{
			state->map_fields_count = parseIntArray(defGetString(def), &state->map_fields);
		}
		else if (strcmp(def->defname, "minfields") == 0)
		{
			state->min_fields = strtol(defGetString(def), NULL, 10);
		}
		else if (strcmp(def->defname, "encoding") == 0)
		{
			state->encoding = pg_char_to_encoding(defGetString(def));
		}
	}

}

/*
 * filePlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
filePlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel)
{
	FdwPlan    *fdwplan;
	MultiCdrExecutionState state;

	/* Fetch options */
	fileGetOptions(foreigntableid, &state);

	/* Construct FdwPlan with cost estimates */
	fdwplan = makeNode(FdwPlan);
	estimate_costs(root, baserel, &state,
				   &fdwplan->startup_cost, &fdwplan->total_cost);
	fdwplan->fdw_private = NIL; /* not used */

	return fdwplan;
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

	ExplainPropertyText("Foreign Directory", state.directory, es);
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

	festate->files = NIL;
	festate->current_file = NULL;

	dir = opendir( festate->directory );

	if (!dir)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
		    errmsg("no directory found %s", dir)));
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
					(errcode(ERRCODE_NO_DATA_FOUND),
			    errmsg("can't retrieve file information %s", path)));
			closedir( dir );
			return -1;
		}
		if ((file_stat.st_mode & S_IFREG) != 0)
		{
			wpath = (pg_wchar*) palloc((strlen(path) + 1) * sizeof(pg_wchar));
			pg_mb2wchar(path, wpath);

			err = pg_regexec(&festate->pattern_regex, wpath, pg_wchar_strlen(wpath), 0, NULL, 0, NULL, 0);
			if (err)
			{
				elog(MULTICDR_FDW_TRACE_LEVEL, "skip unmatched file %s", path);
			}
			else
			{
				festate->files = lappend(festate->files, strdup(path));
			}
			pfree(wpath);
		}
	}

	closedir( dir );
	return 0;
}

static void
beginScan(MultiCdrExecutionState *festate, ForeignScanState *node)
{
	ListCell *cell;
	int i;

	festate->relation_columns_count = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts;

	/* find a column with filename, may be -1 if none specified */
	festate->file_field_column = -1;
	if (festate->file_field && strlen(festate->file_field))
	{
		for (i = 0; i < festate->relation_columns_count; ++i)
		{
			if (!strcmp(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->attrs[i]->attname.data, festate->file_field))
				festate->file_field_column = i;
		}
	}
	elog(MULTICDR_FDW_TRACE_LEVEL, "Field with a filename: %d", festate->file_field_column);

	/* if none provided, create default mapping - one-to-one for existing fields */
	if (festate->map_fields_count == 0)
	{
		festate->map_fields_count = festate->relation_columns_count;
		festate->map_fields = palloc(festate->map_fields_count * sizeof(int));
		for (i = 0; i < festate->map_fields_count; ++i)
			festate->map_fields[i] = i;
	}

	/* set up memory buffers */
	festate->read_buf_size = MULTICDR_FDW_INITIAL_BUF_SIZE;
	festate->read_buf = palloc(festate->read_buf_size);
	festate->file_buf = palloc(MULTICDR_FDW_FILEBUF_SIZE);
	festate->file_buf_start = festate->file_buf_end = festate->file_buf;

	/* alloc fictional arrays so they saved a memory context */
	festate->fields_start = palloc(1 * sizeof(char*));
	festate->fields_end = palloc(1 * sizeof(char*));

	festate->recnum = 0;
	festate->cdr_columns_count = 0;

	/* get list of files */
	enumerateFiles( festate );

	foreach (cell, festate->files)
	{
		elog(MULTICDR_FDW_TRACE_LEVEL, "found file: %s", (char*)lfirst(cell));
	}

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
	
	elog(MULTICDR_FDW_TRACE_LEVEL, "current file: %s", lfirst(festate->current_file));

	if (!is_first)
	{
		Assert(festate->source > 0);
		if (festate->source > 0)
			close(festate->source);
	}

	/* open a file */
	festate->source = multicdr_open( lfirst(festate->current_file), MULTICDR_FDW_OPEN_FLAGS);
	if (festate->source == -1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
				errmsg("unable to open file")));

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
		if (festate->map_fields)
			pfree(festate->map_fields);

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
		pg_regfree(&festate->pattern_regex);
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
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	festate = (MultiCdrExecutionState *) palloc(sizeof(MultiCdrExecutionState));
	/* Fetch options of foreign table */
	fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation), festate);

	node->fdw_state = (void *) festate;

	beginScan( festate, node );
}

static void
fileErrorCallback(void *arg)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState*) arg;
	char* fn;

	fn = festate->current_file ? lfirst(festate->current_file) : "<none>";
	errcontext("MultiCDR Foreign Table filename: '%s' record: %d", fn, festate->recnum);
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
	bool	found;
	ErrorContextCallback errcontext;

	/* Set up callback to identify error line number. */
	errcontext.callback = fileErrorCallback;
	errcontext.arg = (void *) festate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file, we
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
	    errmsg("error reading from external data file %s", lfirst(festate->current_file))));
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

static bool
fetchLine(MultiCdrExecutionState *festate)
{
	if (fetchLineFromFile(festate))
		return true;

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

	if (!*string)
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
			    errmsg("illegal string")));
		(*vals)[count] = result;
		while (*end == ' ')
			++end;
		if (!*end)
				break;
		if (*end != ',')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
			    	errmsg("illegal string")));
		start = end + 1;
		if (count >= initial_count)
			*vals = repalloc(*vals, count * 1.4 * sizeof(int));
	}
	
	return count+1;
}

/* 
 * Count cdr fields in the line
 * Fields are delimited by couple of spaces
 */
static int
parseLine(char* read_buf, char **fields_start, char **fields_end, int max_fields)
{
	int	cdr_columns;
	char *start, *end;
	char  *p, *string_end;

	p = read_buf;
	string_end = read_buf + strlen(read_buf);

	/* count all cdr fields*/

	cdr_columns = 0;
	for (;;)
	{
		for (start = p; start && start != string_end && *start == ' '; ++start)
			;
		for (end = start; end && end != string_end && *end != ' '; ++end)
			;

		if (start == end)
			break;

		if (max_fields && cdr_columns >= max_fields)
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					errmsg("column %d is out of range %d", cdr_columns, max_fields)));
		}

		if (fields_start)
			fields_start[cdr_columns] = start;
		if (fields_end)
			fields_end[cdr_columns] = end;

		p = end;
		++cdr_columns;
	}

	return cdr_columns;
}



/*
 * Rewind buffer to a good sized CDR row
 * Post-condition: read_buf contains good cdr line with `cdr_columns_count` columns
 * Returns true if ok, false if EOF
 * Note: cdr_row is 1-based
 */
static bool 
rewindToCdrLine(MultiCdrExecutionState *festate)
{
	int	cdr_columns;
	int i;

	for (;;)
	{
		if (!fetchLine(festate))
			return false;
	
		++festate->cdr_row;
		
		cdr_columns = parseLine(festate->read_buf, NULL, NULL, 0);

		/* save real columns count */
		if (cdr_columns >= festate->min_fields && festate->cdr_columns_count == 0)
		{
			elog(MULTICDR_FDW_TRACE_LEVEL, "detected CDR columns count %d with min=%d", cdr_columns, festate->min_fields);
			festate->cdr_columns_count = cdr_columns;
			festate->fields_start = repalloc(festate->fields_start, festate->cdr_columns_count * sizeof(char*));
			festate->fields_end = repalloc(festate->fields_end, festate->cdr_columns_count * sizeof(char*));

			/* verify that a mapping is feasible */
			for (i = 0; i < festate->map_fields_count; ++i)
			{
				if (festate->map_fields[i] < 0 || festate->map_fields[i] >= festate->cdr_columns_count)
				{
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("can't map field #%d to CDR field #%d", i, festate->map_fields[i])));
				}
			}
		}

		/* column with a good column count */
		if (festate->cdr_columns_count == cdr_columns)
			break;
		
		/*elog(WARNING, "skip row %d with %d columns (instead of %d)",
				festate->cdr_row, cdr_columns, festate->cdr_columns_count );*/
	}

	return true;
}

/*
 * Construct the text array from the read in data, and stash it in the slot 
 */ 
static bool 
makeTuple(MultiCdrExecutionState *festate, TupleTableSlot *slot)
{
	int	column, cdr_field_mapped;
	char *start, *end, *conv, *temp;

	if (!rewindToCdrLine(festate))
		return false;
	
	parseLine(festate->read_buf, festate->fields_start, festate->fields_end, festate->cdr_columns_count);
	
	for (column = 0; column < festate->relation_columns_count; ++column)
	{
		/* save a current filename if asked */
		if (column == festate->file_field_column)
		{
			slot->tts_isnull[column] = false;
			slot->tts_values[column] = PointerGetDatum(cstring_to_text(lfirst(festate->current_file)));
		}
		else
		{
			cdr_field_mapped = festate->map_fields[column];
			start = festate->fields_start[cdr_field_mapped];
			end = festate->fields_end[cdr_field_mapped];

			/*elog(MULTICDR_FDW_TRACE_LEVEL, "mapped column %d to field %d, %x:%x (%d)", column, cdr_field_mapped, start, end, end-start);*/

			slot->tts_isnull[column] = start == end;
			if (start == end)
			{
				/* precaution, should never happens because it's a good line there can't be empty columns */
				slot->tts_values[column] = PointerGetDatum(NULL);
			}
			else
			{
				temp = pnstrdup(start, end-start);
				conv = pg_any_to_server(temp, end-start, festate->encoding);
				slot->tts_values[column] = PointerGetDatum(cstring_to_text(conv));
				pfree(temp);
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
}

/*
 * fileReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
fileReScanForeignScan(ForeignScanState *node)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState *) node->fdw_state;

	endScan( festate );
	beginScan( festate, node );
}

/*
 * Estimate costs of scanning a foreign table.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
		MultiCdrExecutionState *state,
		Cost *startup_cost, Cost *total_cost)
{
/*	struct stat stat_buf;
	BlockNumber pages;
	int		tuple_width;
	double	ntuples;
	double	nrows;
	Cost	run_cost = 0;
	Cost	cpu_per_tuple;*/

#if 0
	/*
	 * Get size of the file.  It might not be there at plan time, though, in
	 * which case we have to use a default estimate.
	 */
	if (stat(filename, &stat_buf) < 0)
		stat_buf.st_size = 10 * BLCKSZ;

	/*
	 * Convert size to pages for use in I/O cost estimate below.
	 */
	pages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;

	/*
	 * Estimate the number of tuples in the file.  We back into this estimate
	 * using the planner's idea of the relation width; which is bogus if not
	 * all columns are being read, not to mention that the text representation
	 * of a row probably isn't the same size as its internal representation.
	 * FIXME later.
	 */
	tuple_width = MAXALIGN(baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));

	ntuples = clamp_row_est((double) stat_buf.st_size / (double) tuple_width);

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.	This is pretty bogus too, since the planner
	 * will have no stats about the relation, but it's better than nothing.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;

	/*
	 * Now estimate costs.	We estimate costs almost the same way as
	 * cost_seqscan(), thus assuming that I/O costs are equivalent to a
	 * regular table file of the same size.  However, we take per-tuple CPU
	 * costs as 10x of a seqscan, to account for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
#endif

	*startup_cost = baserel->baserestrictcost.startup;
	*total_cost = *startup_cost * 10;
	baserel->rows = 1;
}
