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
	char	   *directory;		/* directory to read */
	char	   *pattern;			/* filename pattern */
	char	   *cdrfields;		/* raw cdr fields mapping */
	List	   *options;		/* merged COPY options, excluding custom options */
	CopyState	cstate;			/* state of reading file */
} MultiCdrExecutionState;

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
	char *directory, *pattern, *cdrfields;
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

/*
 * fileBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
fileBeginForeignScan(ForeignScanState *node, int eflags)
{
	CopyState	cstate;
	MultiCdrExecutionState *festate;
	char *filename_fake = NULL;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	festate = (MultiCdrExecutionState *) palloc(sizeof(MultiCdrExecutionState));
	/* Fetch options of foreign table */
	fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation), festate);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(node->ss.ss_currentRelation,
						   filename_fake,
						   NIL,
						   festate->options);

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate->cstate = cstate;

	node->fdw_state = (void *) festate;
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
	bool		found;
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
	found = NextCopyFrom(festate->cstate, NULL,
						 slot->tts_values, slot->tts_isnull,
						 NULL);
	if (found)
		ExecStoreVirtualTuple(slot);

	/* Remove error callback. */
	error_context_stack = errcontext.previous;

	return slot;
}

/*
 * fileEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
fileEndForeignScan(ForeignScanState *node)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState *) node->fdw_state;

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
		EndCopyFrom(festate->cstate);
}

/*
 * fileReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
fileReScanForeignScan(ForeignScanState *node)
{
	MultiCdrExecutionState *festate = (MultiCdrExecutionState *) node->fdw_state;
	char *filename_fake = NULL;

	EndCopyFrom(festate->cstate);

	festate->cstate = BeginCopyFrom(node->ss.ss_currentRelation,
									filename_fake,
									NIL,
									festate->options);
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
