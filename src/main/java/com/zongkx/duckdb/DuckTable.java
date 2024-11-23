package com.zongkx.duckdb;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class DuckTable extends Table<DuckDBDatabase, DuckSchema> {
    private static final Log LOG = LogFactory.getLog(DuckTable.class);

    protected DuckTable(JdbcTemplate jdbcTemplate, DuckDBDatabase database, DuckSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE " + database.quote(schema.getName(), name) + " ");
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForBoolean("SELECT EXISTS (\n" +
                "  SELECT 1\n" +
                "  FROM   pg_catalog.pg_class c\n" +
                "  JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                "  WHERE  n.nspname = ?\n" +
                "  AND    c.relname = ?\n" +
                "  AND    c.relkind = 'r'\n" + // only tables
                ")", schema.getName(), name);
    }


    @Override
    protected void doLock() {
        LOG.debug("Unable to lock " + this + " as SQLite does not support locking. No concurrent migration supported.");
    }
}