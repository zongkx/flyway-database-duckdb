package com.zongkx.duckdb;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class DuckConnection extends Connection<DuckDBDatabase> {

    protected DuckConnection(DuckDBDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return "main";
    }

    @Override
    public Schema getSchema(String name) {
        return new DuckSchema(jdbcTemplate, database, name);
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        return execute(callable);
    }

    private <T> T execute(Callable<T> callable) {
        try {
            return callable.call();
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to acquire Duck advisory lock", e);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new FlywayException(e);
        }
    }

}