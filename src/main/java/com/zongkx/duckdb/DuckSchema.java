package com.zongkx.duckdb;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.database.base.Type;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.*;

/**
 * DuckDB implementation of Schema.
 */
public class DuckSchema extends Schema<DuckDBDatabase, DuckTable> {
    private static final Log LOG = LogFactory.getLog(DuckSchema.class);

    protected DuckSchema(JdbcTemplate jdbcTemplate, DuckDBDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM pg_namespace WHERE nspname=?", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return !jdbcTemplate.queryForBoolean("SELECT EXISTS (\n" +
                "    SELECT c.oid FROM pg_catalog.pg_class c\n" +
                "    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid AND d.deptype = 'e'\n" +
                "    WHERE  n.nspname = ? AND d.objid IS NULL AND c.relkind IN ('r', 'v', 'S', 't')\n" +
                "  UNION ALL\n" +
                "    SELECT t.oid FROM pg_catalog.pg_type t\n" +
                "    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n" +
                "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = t.oid AND d.deptype = 'e'\n" +
                "    WHERE n.nspname = ? AND d.objid IS NULL AND t.typcategory NOT IN ('A', 'C')\n" +
                "  UNION ALL\n" +
                "    SELECT p.oid FROM pg_catalog.pg_proc p\n" +
                "    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n" +
                "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = p.oid AND d.deptype = 'e'\n" +
                "    WHERE n.nspname = ? AND d.objid IS NULL\n" +
                ")", name, name, name);
    }


    @Override
    protected void doClean() throws SQLException {
        for (String statement : generateDropStatementsForViews()) {
            jdbcTemplate.execute(statement);
        }

        for (Table table : allTables()) {
            table.drop();
        }

        for (String statement : generateDropStatementsForBaseTypes(true)) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : generateDropStatementsForRoutines()) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : generateDropStatementsForEnums()) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : generateDropStatementsForDomains()) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : generateDropStatementsForSequences()) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : generateDropStatementsForBaseTypes(false)) {
            jdbcTemplate.execute(statement);
        }

        for (String statement : generateDropStatementsForExtensions()) {
            jdbcTemplate.execute(statement);
        }
    }

    /**
     * Generates the statements for dropping the extensions in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForExtensions() throws SQLException {
        List<String> statements = new ArrayList<>();

        if (extensionsTableExists()) {
            List<String> extensionNames =
                    jdbcTemplate.queryForStringList(
                            "SELECT e.extname " +
                                    "FROM pg_extension e " +
                                    "LEFT JOIN pg_namespace n ON n.oid = e.extnamespace " +
                                    "LEFT JOIN pg_roles r ON r.oid = e.extowner " +
                                    "WHERE n.nspname=? AND r.rolname=?", name, database.doGetCurrentUser());

            for (String extensionName : extensionNames) {
                statements.add("DROP EXTENSION IF EXISTS " + database.quote(extensionName) + " CASCADE");
            }
        }

        return statements;
    }

    private boolean extensionsTableExists() throws SQLException {
        return jdbcTemplate.queryForBoolean(
                "SELECT EXISTS ( \n" +
                        "SELECT 1 \n" +
                        "FROM pg_tables \n" +
                        "WHERE tablename = 'pg_extension');");
    }

    /**
     * Generates the statements for dropping the sequences in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForSequences() throws SQLException {
        List<String> sequenceNames =
                jdbcTemplate.queryForStringList(
                        "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema=?", name);

        List<String> statements = new ArrayList<>();
        for (String sequenceName : sequenceNames) {
            statements.add("DROP SEQUENCE IF EXISTS " + database.quote(name, sequenceName));
        }

        return statements;
    }

    /**
     * Generates the statements for dropping the types in this schema.
     *
     * @param recreate Flag indicating whether the types should be recreated. Necessary for type-function chicken and egg problem.
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForBaseTypes(boolean recreate) throws SQLException {
        List<Map<String, String>> rows =
                jdbcTemplate.queryForList(
                        "select typname, typcategory from pg_catalog.pg_type t "
                                + "left join pg_depend dep on dep.objid = t.oid and dep.deptype = 'e' "
                                + "where (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) "
                                + "and NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) "
                                + "and t.typnamespace in (select oid from pg_catalog.pg_namespace where nspname = ?) "
                                + "and dep.objid is null "
                                + "and t.typtype != 'd'",
                        name);

        List<String> statements = new ArrayList<>();
        for (Map<String, String> row : rows) {
            statements.add("DROP TYPE IF EXISTS " + database.quote(name, row.get("typname")) + " CASCADE");
        }

        if (recreate) {
            for (Map<String, String> row : rows) {
                // Only recreate Pseudo-types (P) and User-defined types (U)
                if (Arrays.asList("P", "U").contains(row.get("typcategory"))) {
                    statements.add("CREATE TYPE " + database.quote(name, row.get("typname")));
                }
            }
        }

        return statements;
    }

    /**
     * Generates the statements for dropping the routines in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForRoutines() throws SQLException {
        // #2193: PostgreSQL 11 removed the 'proisagg' column and replaced it with 'prokind'.
        String isAggregate = database.getVersion().isAtLeast("11") ? "pg_proc.prokind = 'a'" : "pg_proc.proisagg";
        // PROCEDURE is only available from PostgreSQL 11
        String isProcedure = database.getVersion().isAtLeast("11") ? "pg_proc.prokind = 'p'" : "FALSE";

        List<Map<String, String>> rows =
                jdbcTemplate.queryForList(
                        // Search for all functions
                        "SELECT proname, oidvectortypes(proargtypes) AS args, " + isAggregate + " as agg, " + isProcedure + " as proc "
                                + "FROM pg_proc INNER JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid) "
                                // that don't depend on an extension
                                + "LEFT JOIN pg_depend dep ON dep.objid = pg_proc.oid AND dep.deptype = 'e' "
                                + "WHERE ns.nspname = ? AND dep.objid IS NULL",
                        name
                );

        List<String> statements = new ArrayList<>();
        for (Map<String, String> row : rows) {
            String type = "FUNCTION";
            if (isTrue(row.get("agg"))) {
                type = "AGGREGATE";
            } else if (isTrue(row.get("proc"))) {
                type = "PROCEDURE";
            }
            statements.add("DROP " + type + " IF EXISTS "
                    + database.quote(name, row.get("proname")) + "(" + row.get("args") + ") CASCADE");
        }
        return statements;
    }

    private boolean isTrue(String agg) {
        return agg != null && agg.toLowerCase(Locale.ENGLISH).startsWith("t");
    }

    /**
     * Generates the statements for dropping the enums in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForEnums() throws SQLException {
        List<String> enumNames =
                jdbcTemplate.queryForStringList(
                        "SELECT t.typname FROM pg_catalog.pg_type t INNER JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = ? and t.typtype = 'e'", name);

        List<String> statements = new ArrayList<>();
        for (String enumName : enumNames) {
            statements.add("DROP TYPE " + database.quote(name, enumName));
        }

        return statements;
    }

    /**
     * Generates the statements for dropping the domains in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForDomains() throws SQLException {
        List<String> domainNames =
                jdbcTemplate.queryForStringList(
                        "SELECT t.typname as domain_name\n" +
                                "FROM pg_catalog.pg_type t\n" +
                                "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n" +
                                "       LEFT JOIN pg_depend dep ON dep.objid = t.oid AND dep.deptype = 'e'\n" +
                                "WHERE t.typtype = 'd'\n" +
                                "  AND n.nspname = ?\n" +
                                "  AND dep.objid IS NULL"
                        , name);

        List<String> statements = new ArrayList<>();
        for (String domainName : domainNames) {
            statements.add("DROP DOMAIN IF EXISTS " + database.quote(name, domainName) + " CASCADE");
        }

        return statements;
    }


    /**
     * Generates the statements for dropping the views in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForViews() throws SQLException {
        List<String> viewNames =
                jdbcTemplate.queryForStringList(
                        // Search for all views
                        "SELECT relname FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace" +
                                // that don't depend on an extension
                                " LEFT JOIN pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'" +
                                " WHERE c.relkind = 'v' AND  n.nspname = ? AND dep.objid IS NULL",
                        name);
        List<String> statements = new ArrayList<>();
        for (String domainName : viewNames) {
            statements.add("DROP VIEW IF EXISTS " + database.quote(name, domainName) + " CASCADE");
        }

        return statements;
    }

    @Override
    protected void doCreate() {
        LOG.info("DuckDB does not support creating schemas. Schema not created: " + name);
    }

    @Override
    protected void doDrop() {
        LOG.info("DuckDB does not support dropping schemas. Schema not dropped: " + name);
    }


    @Override
    protected DuckTable[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList("SELECT table_name FROM  information_schema.tables  WHERE table_type='BASE TABLE'");
        DuckTable[] tables = new DuckTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new DuckTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;

    }

    @Override
    public Table getTable(String tableName) {
        return new DuckTable(jdbcTemplate, database, this, tableName);
    }

    @Override
    protected Type getType(String typeName) {
        return new Type(jdbcTemplate, database, this, typeName) {
            @Override
            public void doDrop() throws SQLException {
                LOG.debug("Drop type does not support");
            }
        };
    }
}
