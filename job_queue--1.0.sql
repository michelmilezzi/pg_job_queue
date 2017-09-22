-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION job_queue" to load this file. \quit

CREATE TABLE jobs_queue
(
  id bigserial NOT NULL PRIMARY KEY,
  priority smallint NOT NULL DEFAULT 99,
  queued_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  proc text NOT NULL,
  args jsonb,
  error_count integer NOT NULL DEFAULT 0,
  last_error text
);

CREATE FUNCTION job_queue_launch(name) RETURNS pg_catalog.int4 STRICT AS 'MODULE_PATHNAME'
LANGUAGE C;

--Probably there must be a better way to pass database to SPI backend. For now, this is ok.
CREATE FUNCTION job_queue_launch() RETURNS pg_catalog.int4 STRICT AS $$
  SELECT job_queue_launch(current_database());
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION launch_job_worker() RETURNS TRIGGER AS $$
BEGIN
  PERFORM job_queue_launch();
  RETURN NULL;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER launch_job_worker_tg
  AFTER INSERT
  ON jobs_queue
  FOR EACH STATEMENT
  EXECUTE PROCEDURE launch_job_worker();

