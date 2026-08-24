-- Environment catalog for the dbt Silver/Gold layers - dev/qa/prod separation happens at the
-- catalog level (Free Edition has one metastore, so no separate databases). Idempotent. Uses
-- {{CATALOG}} so the same file creates whichever catalog the branch maps to (dev_catalog on
-- main, qa_catalog on main_qa, prod_catalog on main_prod) - see databricks-env-deploy.yml.

CREATE CATALOG IF NOT EXISTS {{CATALOG}};
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.silver_flights;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.gold_flights;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.master;

-- full access for a human account. Skipped if ADMIN_PRINCIPAL is unset.
GRANT ALL PRIVILEGES, MANAGE ON CATALOG {{CATALOG}} TO {{ADMIN_PRINCIPAL}};

