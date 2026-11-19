### Summary: Airflow `3.4.0+astro.1` Startup Timeout & Migration Issue

- **What Happened:** After updating the project to **Astro Runtime 3.4-1** (`3.4.0+astro.1`), `astro dev start` timed out on the `api-server` health check and the container kept restarting.
- **Root Cause:** Logs showed that Airflow `3.4.0+astro.1` required a database schema migration (`airflow db migrate`). During that migration, updating `task_instance` IDs failed in PostgreSQL with `psycopg.errors.UndefinedFunction: function gen_random_uuid() does not exist`.
- **Why `pgcrypto` Was Necessary:** This was needed specifically because the database was running **PostgreSQL 12.22** (`PostgreSQL 12.22 (Debian 12.22-1.pgdg120+1)`). `gen_random_uuid()` was only added as a built-in function in PostgreSQL 13; on PostgreSQL 12, it requires enabling the `pgcrypto` extension.
- **How It Was Solved:**

1. Connected to the PostgreSQL container (`psql -U postgres`) and ran `CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA public;` to provide `gen_random_uuid()` on PostgreSQL 12.22.
2. Ran `astro dev restart` to bring the environment back up cleanly.
3. Ran `astro dev run airflow db migrate` to finish migrating the Airflow `3.4.0+astro.1` schema, resolving the `api-server` restart loop.
