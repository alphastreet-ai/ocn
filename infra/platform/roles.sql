-- Roles and schemas on the shared platform database (alphaStreet-api docs/PLATFORM_ARCHITECTURE.md
-- §4.2). Idempotent: rerun after every edit. Run as master against database `platform`:
--   SVC_APP_PASSWORD=... psql "host=... dbname=platform user=master sslmode=verify-full" \
--     -v ON_ERROR_STOP=1 -f roles.sql
-- (bootstrap-app.sh does this; the password comes from the environment, never argv.)
-- Today: schema app only. Each OCN service adds its schema + svc_<name> here when it moves in.

-- Nobody connects or creates in public by default; each role is granted what it needs.
REVOKE ALL ON DATABASE platform FROM PUBLIC;
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

GRANT CONNECT ON DATABASE platform TO svc_app;
GRANT USAGE ON SCHEMA public TO svc_app;
-- RDS master isn't a superuser: PG16 needs SET ROLE on svc_app to hand it a schema.
GRANT svc_app TO CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION svc_app;
ALTER SCHEMA app OWNER TO svc_app;
