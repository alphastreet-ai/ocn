-- Roles and schemas on the shared platform database (alphaStreet-api docs/PLATFORM_ARCHITECTURE.md
-- §4.2). Idempotent: rerun after every edit. Run as master against database `alphastreet`:
--   SVC_APP_PASSWORD=... SVC_SIM_PASSWORD=... psql "host=... dbname=alphastreet user=master sslmode=verify-full" \
--     -v ON_ERROR_STOP=1 -f roles.sql
-- (bootstrap-app.sh does this; the password comes from the environment, never argv.)
-- Schemas: app (alphaStreet-api), sim (alphastreet-simulation). Each OCN service adds its schema + svc_<name> here when it moves in.

-- Nobody connects or creates in public by default; each role is granted what it needs.
REVOKE ALL ON DATABASE alphastreet FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Extensions live in public (search_path = <schema>, public). Created here so no service role
-- needs CREATE on the database; alphaStreet-api's migration 0005 then finds it.
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


-- --- app: alphaStreet-api ---
-- svc_app owns its schema and runs its own migrations (the deploy's one-off alembic task uses the
-- same DATABASE_URL). ponytail: no separate migrator_app until a second writer needs one.

\getenv svc_app_password SVC_APP_PASSWORD
SELECT 'CREATE ROLE svc_app LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_app') \gexec

ALTER ROLE svc_app WITH LOGIN PASSWORD :'svc_app_password' CONNECTION LIMIT 40;
ALTER ROLE svc_app SET search_path = app, public;
-- The app sets its own statement_timeout per connection; this caps everything else (psql, alembic).
ALTER ROLE svc_app SET statement_timeout = '5min';
ALTER ROLE svc_app SET idle_in_transaction_session_timeout = '60s';

GRANT CONNECT ON DATABASE alphastreet TO svc_app;
GRANT USAGE ON SCHEMA public TO svc_app;
-- RDS master isn't a superuser: PG16 needs SET ROLE on svc_app to hand it a schema.
GRANT svc_app TO CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION svc_app;
ALTER SCHEMA app OWNER TO svc_app;


-- --- sim: alphastreet-simulation (engine + regional pollers) ---
-- svc_sim owns sim and creates its tables there (alphastreet-simulation db/sim/*.sql, run by its
-- migrate task). Readers get SELECT on exactly what they need (PLATFORM_ARCHITECTURE §4.2).

\getenv svc_sim_password SVC_SIM_PASSWORD
SELECT 'CREATE ROLE svc_sim LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_sim') \gexec

ALTER ROLE svc_sim WITH LOGIN PASSWORD :'svc_sim_password' CONNECTION LIMIT 20;
ALTER ROLE svc_sim SET search_path = sim, public;
ALTER ROLE svc_sim SET statement_timeout = '5min';
ALTER ROLE svc_sim SET idle_in_transaction_session_timeout = '60s';

GRANT CONNECT ON DATABASE alphastreet TO svc_sim;
GRANT USAGE ON SCHEMA public TO svc_sim;
GRANT svc_sim TO CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS sim AUTHORIZATION svc_sim;
ALTER SCHEMA sim OWNER TO svc_sim;

-- alphaStreet-api polls sim.runs for ready runs (RUN_SOURCE=sim_runs) and reads the B7 result tables
-- sim.company_results + sim.forecasts (RUN_DATA=db). SELECT only, granted per table (no ALTER DEFAULT
-- PRIVILEGES), so engine-internal tables like sim.regional_signals stay private. The sim migrate task
-- creates them, so each grant applies on the first bootstrap-app.sh run after it (migrate, then bootstrap).
GRANT USAGE ON SCHEMA sim TO svc_app;
SELECT 'GRANT SELECT ON sim.runs TO svc_app'
WHERE to_regclass('sim.runs') IS NOT NULL \gexec
SELECT 'GRANT SELECT ON sim.company_results TO svc_app'
WHERE to_regclass('sim.company_results') IS NOT NULL \gexec
SELECT 'GRANT SELECT ON sim.forecasts TO svc_app'
WHERE to_regclass('sim.forecasts') IS NOT NULL \gexec
