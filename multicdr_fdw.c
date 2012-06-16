/*-------------------------------------------------------------------------
 *
 * multicdr_fdw.c
 *		  foreign-data wrapper for server-side flat files.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "optimizer/cost.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MultiCdrFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
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
	{"cdrfields", ForeignTableRelationId},

	/* Format options */
	/* oids option is not supported */
	{"format", ForeignTableRelationId},
	{"header", ForeignTableRelationId},
	{"delimiter", ForeignTableRelationId},
	{"quote", ForeignTableRelationId},
	{"escape", ForeignTableRelationId},
	{"null", ForeignTableRelationId},
	{"encoding", ForeignTableRelationId},

	/*
	 * force_quote is not supported by multicdr_fdw because it's for COPY TO.
	 */

	/*
	 * force_not_null is not supported by multicdr_fdw.  It would need a parser
	 * for list of columns, not to mention a way to check the column list
	 * against the table.
	 */

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct MultiCdrExecutionState
{
	char		*directory;		/* directory to read */
	char		*pattern;			/* filename pattern */
	char		*cdrfields;		/* raw cdr fields mapping */
	List		*options;		/* merged COPY options, excluding custom options */
	CopyState	cstate;			/* state of reading file */
	
	/* context */
	char           *read_buf;
	Datum          *text_array_values;
	bool           *text_array_nulls;
	int             recnum;

	List			*files;
	ListCell	*currentFile;
} MultiCdrExecutionState;

#define FILE_FDW_TEXTARRAY_STASH_INIT 64

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
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char *directory = NULL, *pattern = NULL, *cdrfields = NULL;
	List	   *other_options = NIL;
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
		else if (strcmp(def->defname, "cdrfields") == 0)
		{
			if (cdrfields)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cdrfields = defGetString(def);
		}
		else
			other_options = lappend(other_options, def);
	}

	/*
	 * Now apply the core COPY code's validation logic for more checks.
	 */
	ProcessCopyOptions(NULL, true, other_options);

	/*
	 * Options that are required for multicdr_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && directory == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("directory is required for multicdr_fdw foreign tables")));
	if (catalog == ForeignTableRelationId && pattern == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("pattern is required for multicdr_fdw foreign tables")));
	if (catalog == ForeignTableRelationId && cdrfields == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("cdrfields is required for multicdr_fdw foreign tables")));

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
	List	   *options, *new_options;
	ListCell   *lc;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * multicdr_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
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
	state->directory = state->pattern = state->cdrfields = NULL;
	new_options = NIL;
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "directory") == 0)
		{
			state->directory = defGetString(def);
		}
		else if (strcmp(def->defname, "pattern") == 0)
		{
			state->pattern = defGetString(def);
		}
		else if (strcmp(def->defname, "cdrfields") == 0)
		{
			state->cdrfields = defGetString(def);
		}
		else
			new_options = lappend(new_options, def);
	}

	/*
	 * The validator should have checked that all options are included in the
	 * options, but check again, just in case.
	 */
	if (state->directory == NULL)
		elog(ERROR, "directory is required for multicdr_fdw foreign tables");
	if (state->pattern == NULL)
		elog(ERROR, "pattern is required for multicdr_fdw foreign tables");
	if (state->cdrfields == NULL)
		elog(ERROR, "cdrfields is required for multicdr_fdw foreign tables");

	state->options = new_options;
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
	ExplainPropertyText("Foreign Pattern", state.pattern, es);
	ExplainPropertyText("Foreign CDR Fields", state.cdrfields, es);

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
	}
}

static int
enumerateFiles (MultiCdrExecutionState *state)
{
	DIR *dir;
	struct dirent *de;
	struct stat file_stat;
	const int buf_size = 4096;
	char full_name[buf_size];
	int status;

	state->files = NIL;
	state->currentFile = NIL;

	dir = opendir( state->directory );

	if (!dir)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("no directory found %s", dir)));
		return -1;
	}

	while ((de = readdir( dir )) != NULL)
	{
		strncpy( full_name, state->directory, sizeof(full_name)-1 );
		strncat( full_name, "/", sizeof(full_name)-1 );
		strncat( full_name, de->d_name, sizeof(full_name)-1 );
		
		if (stat( full_name, &file_stat ))
		{
			ereport(ERROR,
					(errcode(ERRCODE_NO_DATA_FOUND),
					 errmsg("can't retrieve file information %s", full_name)));
			closedir( dir );
			return -1;
		}
		if ((file_stat.st_mode & S_IFREG) != 0)
		{
			state->files = lappend(state->files, strdup(full_name));
		}
	}
	state->currentFile = (char*)lfirst(list_head(state->files));

	closedir( dir );
	return 0;
}

static void
beginScan(MultiCdrExecutionState *festate, ForeignScanState *node)
{
	char *filename;
	ListCell *cell;

	enumerateFiles( festate );

	foreach (cell, festate->files)
	{
		filename = (char*) lfirst(cell);
		elog(NOTICE, "found file: %s", filename);
	}
	elog(NOTICE, "current file: %s", festate->currentFile);

	festate->cstate = NULL;
	if (festate->currentFile)
	{
		/* Create CopyState from FDW options.  We always acquire all columns, so
		 * as to match the expected ScanTupleSlot signature.
		 */
		festate->cstate = BeginCopyFrom(node->ss.ss_currentRelation,
								 festate->currentFile,
								 NIL,
								 festate->options);

		/* set up the work area we'll use to construct the array */
		festate->text_array_stash_size = FILE_FDW_TEXTARRAY_STASH_INIT;
		festate->text_array_values =
			palloc(FILE_FDW_TEXTARRAY_STASH_INIT * sizeof(Datum));
		festate->text_array_nulls =
			palloc(FILE_FDW_TEXTARRAY_STASH_INIT * sizeof(bool));
	}
}

static void
endScan(MultiCdrExecutionState *festate)
{
	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		if (festate->cstate)
		{
			EndCopyFrom(festate->cstate);
			festate->cstate = NULL;
		}

		if (festate->files)
		{
			list_free(festate->files);
			festate->files = NIL;
		}
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

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	node->fdw_state = (void *) festate;

	beginScan( festate, node );
}

#if 0
bool
MyNextCopyFrom(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls, Oid *tupleOid)
{
	TupleDesc	tupDesc;
	Form_pg_attribute *attr;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int			nfields;
	bool		isnull;
	bool		file_has_oids = cstate->file_has_oids;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;

	tupDesc = RelationGetDescr(cstate->rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);
	nfields = file_has_oids ? (attr_count + 1) : attr_count;

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->binary)
	{
		char	  **field_strings;
		ListCell   *cur;
		int			fldct;
		int			fieldno;
		char	   *string;

		/* read raw fields in the next line */
		if (!NextCopyFromRawFields(cstate, &field_strings, &fldct))
			return false;

		/* check for overflowing fields */
		if (nfields > 0 && fldct > nfields)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));

		fieldno = 0;

		/* Loop to read the user attributes on the line. */
		foreach(cur, cstate->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;

			if (fieldno >= fldct)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for column \"%s\"",
								NameStr(attr[m]->attname))));
			string = field_strings[fieldno++];

			if (cstate->csv_mode && string == NULL &&
				cstate->force_notnull_flags[m])
			{
				/* Go ahead and read the NULL string */
				string = cstate->null_print;
			}

			cstate->cur_attname = NameStr(attr[m]->attname);
			cstate->cur_attval = string;
			values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  attr[m]->atttypmod);
			if (string != NULL)
				nulls[m] = false;
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		Assert(fieldno == nfields);
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.	Anything not processed here or above will
	 * remain NULL.
	 */
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]], NULL);
	}

	return true;
}
#endif

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
	bool		found;
	char          **raw_fields;
	int             nfields;
	ErrorContextCallback errcontext;

	/* Set up callback to identify error line number. */
	errcontext.callback = CopyFromErrorCallback;
	errcontext.arg = (void *) festate->cstate;
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
	found = NextCopyFromRawFields(festate->cstate, &raw_fields, &nfields);
	if (found)
	{
		makeTextArray(festate, slot, raw_fields, nfields);
		ExecStoreVirtualTuple(slot);
	}

	/*if (found)
	{
		elog(NOTICE, "tuple found 0: %s; %d", DatumGetCString(slot->tts_values+0), slot->tts_isnull[0]);
		elog(NOTICE, "tuple found 1: %s; %d", DatumGetCString(slot->tts_values+1), slot->tts_isnull[1]);
	}
	else
		elog(NOTICE, "tuple not found");*/


	/* Remove error callback. */
	error_context_stack = errcontext.previous;

	return slot;
}

static bool
NextCopyFromRawFieldsSingle(MultiCdrExecutionState *festate)
{
	int nread;

	nread = read(festate->source, festate->read_buf, festate->read_len);
	if (nread == 0)
		return false;
	else if (nread != festate->read_len)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH),
				 errmsg("error reading fixed length record")));

	return true;
}

/*
 * Construct the text array from the read in data, and stash it in the slot 
 */ 
static void 
makeTextArray(FileFixedLengthFdwExecutionState *festate, TupleTableSlot *slot)
{
	Datum     *values;
	bool      *nulls;
	int        dims[1];
	int        lbs[1];
	int        fld;
	Datum      result;
	int        fldct = festate->nfields;
	char      *string = festate->read_buf;
	values = festate->text_array_values;
	nulls = festate->text_array_nulls;

	dims[0] = fldct;
	lbs[0] = 1; /* sql arrays typically start at 1 */

	for (fld=0; fld < fldct; fld++)
	{
		char *start = string;
		char *conv;
		int  len = festate->field_lengths[fld];
		int  slen = len, i;

		if (festate->trim_all_fields)
		{
			/* skip leading and trailing spaces if required */
			for (i = 0; i < slen && *start == ' '; i++)
			{
					start++;
					len--;
			}
			while(len > 0 && start[len-1] == ' ')
				len--;
		}

		/*
		 * pg_any_to_server will both validate that the input is
		 * ok in the named encoding and translate it from that into the
		 * current server encoding.
		 *
		 * If the string handed back is what we passed in it won't
		 * be null terminated, but if we get back something else
		 * it will be (but the length will be unknown);
		 */
		conv = pg_any_to_server(start, len, festate->encoding);
		if (conv == start)
			values[fld] = PointerGetDatum(cstring_to_text_with_len(conv,len));
		else
			values[fld] = PointerGetDatum(cstring_to_text(conv));

		string += festate->field_lengths[fld];
	}

	result = PointerGetDatum(construct_md_array(
								 values, 
								 nulls,
								 1,
								 dims,
								 lbs,
								 TEXTOID,
								 -1,
								 false,
								 'i'));

	slot->tts_values[0] = result;
	slot->tts_isnull[0] = false;

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
	struct stat stat_buf;
	BlockNumber pages;
	int			tuple_width;
	double		ntuples;
	double		nrows;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

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
	baserel->rows = 10;
}
