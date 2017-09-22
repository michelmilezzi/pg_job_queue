/* -------------------------------------------------------------------------
 *
 * Job queue mechanism
 * 
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "pgstat.h"
#include "utils/snapmgr.h"
#include "commands/async.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(job_queue_launch);

void job_queue_main(void) pg_attribute_noreturn();

static
void job_consumer(StringInfoData buf)
{

	bool hadJob;
	int	ret;
													
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, buf.data);

	ret = SPI_execute(buf.data, false, 1);

	if (ret != SPI_OK_SELECT)
	{
		elog(FATAL, "error grabbing next job from queue, returned code: %d", ret);
	}

	hadJob = SPI_processed > 0 && SPI_tuptable != NULL;

	if (hadJob)
	{
		int retProc;
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		StringInfoData jobProcedure;
		HeapTuple tuple = tuptable->vals[0];
		char *arguments = SPI_getvalue(tuple, tupdesc, 3);

		initStringInfo(&jobProcedure);	
		appendStringInfo(&jobProcedure, "SELECT ");
		appendStringInfo(&jobProcedure, "%s", SPI_getvalue(tuple, tupdesc, 2));
		appendStringInfo(&jobProcedure, "(%s)", arguments != NULL ? arguments : "\0");
		
		elog(LOG, "job found, id: %s proc: %s", SPI_getvalue(tuple, tupdesc, 1), jobProcedure.data);
		pgstat_report_activity(STATE_RUNNING, jobProcedure.data);	
		retProc = SPI_execute(jobProcedure.data, false, 0); //FIXME need to fix error treatment here, e.g: error thrown by the procedure

		if (retProc != SPI_OK_SELECT)
			elog(FATAL, "job procedure error, returned code: %d", retProc);

		elog(LOG, "job finished");		

	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);		
	ProcessCompletedNotifies();

	if (hadJob) 
	{
		job_consumer(buf);		
	}

}

void
job_queue_main(void)
{
	char *databaseName = MyBgworkerEntry->bgw_extra;
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf,
						"WITH next_job AS ( "
						" 	SELECT id, proc, args FROM jobs_queue "
						" 	ORDER BY priority ASC, id ASC "
						" 	FOR UPDATE SKIP LOCKED "
						"	LIMIT 1 "
						"  ), queue_pop AS ( "
						" 	 DELETE FROM jobs_queue WHERE id = (SELECT id FROM next_job) "
						"  ) "
						"SELECT id, proc, string_agg(CASE WHEN value ~ '^[0-9]+$' THEN value ELSE quote_literal(value) END, ',') AS args "
						"FROM next_job LEFT JOIN LATERAL jsonb_array_elements_text(args) ON TRUE GROUP BY id, proc");		

	BackgroundWorkerInitializeConnection(databaseName, NULL);

	elog(LOG, "job worker initialized for database %s", databaseName);
	
	job_consumer(buf);

	elog(LOG, "job worker ended, no more jobs left");

	proc_exit(0);

}

Datum
job_queue_launch(PG_FUNCTION_ARGS)
{

	char *databaseName = PG_GETARG_CSTRING(0);
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t pid;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;
	sprintf(worker.bgw_library_name, "job_queue");
	sprintf(worker.bgw_function_name, "job_queue_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "job worker %d", 1); //TODO probably it's a good idea to instantiate more workers
	memcpy(worker.bgw_extra, databaseName, sizeof(&databaseName));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start worker job"),
			   errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start worker job without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}
