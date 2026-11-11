-- Postgres roles for a TRUF.NETWORK node.
--
-- kwild connects as "kwild". The role owns the "kwild" database and reads its
-- changes over logical replication. It is not a superuser, and it may not run
-- programs on the database server or read and write the server's files.
--
-- tn_vacuum runs pg_repack as "tn_repack", the only role besides superusers
-- that may use the pg_repack extension. It works on kwild's tables as a member
-- of "kwild", and it is not a superuser either.
--
-- Some objects kwild needs can only be created by a superuser: the publication
-- it reads its changes from, the event trigger that gives its tables full
-- replica identity, a second event trigger that does the same for the copy
-- pg_repack makes of a table, and the pg_repack extension. This script creates
-- them and leaves them owned by the superuser that runs it, with the event
-- triggers' functions in the "tn_replica_identity" schema, which kwild may not
-- use.
--
-- Run it as a superuser, such as the "postgres" role of the kwil-postgres
-- image, before kwild starts. It is safe to run again.
--
--   psql -U postgres -v ON_ERROR_STOP=1 -f postgres-roles.sql

\set ON_ERROR_STOP on
SET client_min_messages = warning;

SELECT 'CREATE ROLE kwild LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'kwild')\gexec
ALTER ROLE kwild NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS REPLICATION LOGIN;
-- kwild restores a snapshot with session_replication_role set to replica.
GRANT SET ON PARAMETER session_replication_role TO kwild;

SELECT 'CREATE ROLE tn_repack LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'tn_repack')\gexec
ALTER ROLE tn_repack NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION LOGIN;
GRANT kwild TO tn_repack;

SELECT format('REVOKE %I FROM %I', g.rolname, m.rolname)
FROM pg_auth_members a
JOIN pg_roles g ON g.oid = a.roleid
JOIN pg_roles m ON m.oid = a.member
WHERE m.rolname IN ('kwild', 'tn_repack')
  AND g.rolname IN ('pg_execute_server_program', 'pg_read_server_files', 'pg_write_server_files')\gexec

SELECT 'CREATE DATABASE kwild OWNER kwild'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kwild')\gexec

\connect kwild
SET client_min_messages = warning;
SET search_path = pg_catalog;
-- Do not hold up a running node for long if a lock below is busy.
SET lock_timeout = '10s';

SELECT 'CREATE PUBLICATION kwild_repl FOR ALL TABLES'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'kwild_repl')\gexec
ALTER PUBLICATION kwild_repl OWNER TO CURRENT_USER;

-- The event trigger's function lives in a schema that the superuser owns and
-- kwild may not use. kwild owns this database and so its public schema, and a
-- schema's owner may drop any function in it, along with the event trigger
-- that calls the function.
CREATE SCHEMA IF NOT EXISTS tn_replica_identity;
ALTER SCHEMA tn_replica_identity OWNER TO CURRENT_USER;
REVOKE ALL ON SCHEMA tn_replica_identity FROM PUBLIC;

-- The same function kwild creates when it runs as a superuser, with its search
-- path fixed: it runs for every role that creates a table in this database,
-- superusers included.
CREATE OR REPLACE FUNCTION tn_replica_identity.set_replica_identity()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN
        SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE TABLE'
    LOOP
        EXECUTE 'ALTER TABLE ' || obj.object_identity || ' REPLICA IDENTITY FULL';
    END LOOP;
END;
$$;
ALTER FUNCTION tn_replica_identity.set_replica_identity() OWNER TO CURRENT_USER;

-- An event trigger cannot be pointed at another function. One that calls any
-- other function, such as the public.set_replica_identity() that kwild creates
-- while it is a superuser, is dropped and created again, and the function it
-- called is dropped. So is one that fires on any other event or tags: kwild
-- does not start unless the trigger fires on CREATE TABLE alone. This runs in
-- one transaction, so that no table is created in between without the trigger.
BEGIN;
SELECT 'DROP EVENT TRIGGER set_replica_identity_on_create',
    CASE WHEN evtfoid <> 'tn_replica_identity.set_replica_identity()'::regprocedure
        THEN format('DROP FUNCTION %s', evtfoid::regprocedure) END
FROM pg_event_trigger
WHERE evtname = 'set_replica_identity_on_create'
  AND (evtfoid <> 'tn_replica_identity.set_replica_identity()'::regprocedure
    OR evtevent <> 'ddl_command_end'
    OR evttags IS DISTINCT FROM ARRAY['CREATE TABLE'])\gexec
SELECT 'CREATE EVENT TRIGGER set_replica_identity_on_create ON ddl_command_end '
    'WHEN TAG IN (''CREATE TABLE'') EXECUTE FUNCTION tn_replica_identity.set_replica_identity()'
WHERE NOT EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'set_replica_identity_on_create')\gexec
COMMIT;
ALTER EVENT TRIGGER set_replica_identity_on_create OWNER TO CURRENT_USER;
ALTER EVENT TRIGGER set_replica_identity_on_create ENABLE;

-- pg_repack copies a table with CREATE TABLE ... AS, which the trigger above
-- does not fire on, and while it runs it replays the table's writes on the
-- copy. The copy has no primary key and the publication covers every table, so
-- Postgres refuses the updates and deletes among those writes unless the copy
-- has full replica identity. kwild does not start if the trigger above fires
-- on any other tag or runs another function, so a second trigger covers the
-- copies: the tables that CREATE TABLE ... AS makes in pg_repack's schema, and
-- no other table.
CREATE OR REPLACE FUNCTION tn_replica_identity.set_repack_copy_replica_identity()
RETURNS event_trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN
        SELECT * FROM pg_event_trigger_ddl_commands()
        WHERE command_tag = 'CREATE TABLE AS' AND object_type = 'table' AND schema_name = 'repack'
    LOOP
        EXECUTE 'ALTER TABLE ' || obj.object_identity || ' REPLICA IDENTITY FULL';
    END LOOP;
END;
$$;
ALTER FUNCTION tn_replica_identity.set_repack_copy_replica_identity() OWNER TO CURRENT_USER;

-- As above, a trigger of that name that calls another function, or fires on
-- any other event or tags, is replaced in one transaction.
BEGIN;
SELECT 'DROP EVENT TRIGGER set_replica_identity_on_repack_copy'
FROM pg_event_trigger
WHERE evtname = 'set_replica_identity_on_repack_copy'
  AND (evtfoid <> 'tn_replica_identity.set_repack_copy_replica_identity()'::regprocedure
    OR evtevent <> 'ddl_command_end'
    OR evttags IS DISTINCT FROM ARRAY['CREATE TABLE AS'])\gexec
SELECT 'CREATE EVENT TRIGGER set_replica_identity_on_repack_copy ON ddl_command_end '
    'WHEN TAG IN (''CREATE TABLE AS'') EXECUTE FUNCTION tn_replica_identity.set_repack_copy_replica_identity()'
WHERE NOT EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'set_replica_identity_on_repack_copy')\gexec
COMMIT;
ALTER EVENT TRIGGER set_replica_identity_on_repack_copy OWNER TO CURRENT_USER;
ALTER EVENT TRIGGER set_replica_identity_on_repack_copy ENABLE;

-- An extension that a role other than a superuser created, or the tables and
-- triggers an interrupted pg_repack run left behind, are cleared by creating
-- the extension again. Nothing in the repack schema is consensus state.
SELECT 'DROP EXTENSION pg_repack CASCADE'
FROM pg_extension e
JOIN pg_roles r ON r.oid = e.extowner
WHERE e.extname = 'pg_repack'
  AND (NOT r.rolsuper
    OR EXISTS (SELECT FROM pg_trigger WHERE tgname = 'repack_trigger' AND NOT tgisinternal)
    OR EXISTS (SELECT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'repack' AND c.relkind = 'r'))\gexec
CREATE EXTENSION IF NOT EXISTS pg_repack;
REVOKE ALL ON SCHEMA repack FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA repack FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA repack FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA repack TO tn_repack;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA repack TO tn_repack;
GRANT SELECT ON ALL TABLES IN SCHEMA repack TO tn_repack;
-- pg_repack's trigger function runs as its owner whenever a table that is
-- being repacked is written to, so tn_repack owns it rather than a superuser.
ALTER FUNCTION repack.repack_trigger() OWNER TO tn_repack;

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname IN ('kwild', 'tn_repack') AND rolsuper) THEN
        RAISE EXCEPTION 'kwild and tn_repack must not be superusers';
    END IF;
    IF EXISTS (
        SELECT FROM unnest(ARRAY['kwild', 'tn_repack']) AS m(rolname),
            unnest(ARRAY['pg_execute_server_program', 'pg_read_server_files', 'pg_write_server_files']) AS g(rolname)
        WHERE pg_has_role(m.rolname, g.rolname, 'MEMBER')) THEN
        RAISE EXCEPTION 'kwild and tn_repack must not run programs on the server or use its files';
    END IF;
    IF (SELECT datdba FROM pg_database WHERE datname = 'kwild') <> 'kwild'::regrole THEN
        RAISE EXCEPTION 'the kwild database must be owned by kwild';
    END IF;
    IF has_schema_privilege('kwild', 'repack', 'USAGE') THEN
        RAISE EXCEPTION 'kwild must not use the pg_repack extension';
    END IF;
    -- Owning the event trigger, its function or the function's schema is
    -- enough to drop the trigger.
    IF NOT EXISTS (
        SELECT FROM pg_event_trigger e
        JOIN pg_proc p ON p.oid = e.evtfoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE e.evtname = 'set_replica_identity_on_create' AND e.evtenabled <> 'D'
          AND NOT EXISTS (
            SELECT FROM unnest(ARRAY['kwild', 'tn_repack']) AS m(rolname)
            WHERE pg_has_role(m.rolname, e.evtowner, 'USAGE')
               OR pg_has_role(m.rolname, p.proowner, 'USAGE')
               OR pg_has_role(m.rolname, n.nspowner, 'USAGE'))) THEN
        RAISE EXCEPTION 'kwild and tn_repack must not be able to drop the event trigger that sets replica identity';
    END IF;
    -- kwild checks this at startup.
    IF NOT EXISTS (
        SELECT FROM pg_event_trigger
        WHERE evtname = 'set_replica_identity_on_create' AND evtenabled = 'O'
          AND evtevent = 'ddl_command_end' AND evttags = ARRAY['CREATE TABLE']) THEN
        RAISE EXCEPTION 'the event trigger set_replica_identity_on_create must be enabled and fire on CREATE TABLE alone';
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_event_trigger
        WHERE evtname = 'set_replica_identity_on_repack_copy' AND evtenabled IN ('O', 'A')
          AND evtevent = 'ddl_command_end' AND 'CREATE TABLE AS' = ANY (evttags)) THEN
        RAISE EXCEPTION 'the event trigger set_replica_identity_on_repack_copy must be enabled and fire on CREATE TABLE AS';
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_event_trigger e
        JOIN pg_proc p ON p.oid = e.evtfoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE e.evtname = 'set_replica_identity_on_repack_copy'
          AND NOT EXISTS (
            SELECT FROM unnest(ARRAY['kwild', 'tn_repack']) AS m(rolname)
            WHERE pg_has_role(m.rolname, e.evtowner, 'USAGE')
               OR pg_has_role(m.rolname, p.proowner, 'USAGE')
               OR pg_has_role(m.rolname, n.nspowner, 'USAGE'))) THEN
        RAISE EXCEPTION 'kwild and tn_repack must not be able to drop the event trigger that sets replica identity on pg_repack''s copies';
    END IF;
END
$$;
