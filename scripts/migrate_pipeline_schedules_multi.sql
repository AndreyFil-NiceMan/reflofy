-- Migrates pipeline_schedules from one-row-per-pipeline to
-- one-row-per-(pipeline, named schedule), to support pipelines that declare
-- several ScheduledRun entries (different cron + params) instead of a
-- single `schedule` string.
--
-- Run this manually against any existing deployment's Postgres database
-- BEFORE deploying the app version that ships the `schedules` list. Fresh
-- dev/e2e stacks don't need it: SQLAlchemy's create_all() builds the new
-- shape directly on an empty database.
--
-- Existing rows become the "default" named schedule for their pipeline.
-- After deploy, sync_schedules_from_registry() reconciles the table with
-- each pipeline's declared `schedules`; any schedule renamed away from
-- "default" simply gets a fresh row (next_run_at recomputed from now) and
-- the old "default" row is soft-disabled.

ALTER TABLE pipeline_schedules ADD COLUMN schedule_name VARCHAR(255) NOT NULL DEFAULT 'default';
ALTER TABLE pipeline_schedules ADD COLUMN runtime_params JSON NOT NULL DEFAULT '{}'::json;

ALTER TABLE pipeline_schedules DROP CONSTRAINT pipeline_schedules_pkey;
ALTER TABLE pipeline_schedules ADD PRIMARY KEY (pipeline_name, schedule_name);

ALTER TABLE pipeline_schedules ALTER COLUMN schedule_name DROP DEFAULT;
