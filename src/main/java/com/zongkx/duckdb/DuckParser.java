package com.zongkx.duckdb;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.parser.*;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.core.internal.sqlscript.ParsedSqlStatement;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class DuckParser extends Parser {
    private static final Log LOG = LogFactory.getLog(DuckParser.class);

    private static final Pattern COPY_FROM_STDIN_REGEX = Pattern.compile("^COPY( .*)? FROM STDIN");
    private static final Pattern CREATE_DATABASE_TABLESPACE_SUBSCRIPTION_REGEX = Pattern.compile("^(CREATE|DROP) (DATABASE|TABLESPACE|SUBSCRIPTION)");
    private static final Pattern ALTER_SYSTEM_REGEX = Pattern.compile("^ALTER SYSTEM");
    private static final Pattern CREATE_INDEX_CONCURRENTLY_REGEX = Pattern.compile("^(CREATE|DROP)( UNIQUE)? INDEX CONCURRENTLY");
    private static final Pattern REINDEX_REGEX = Pattern.compile("^REINDEX( VERBOSE)? (SCHEMA|DATABASE|SYSTEM)");
    private static final Pattern VACUUM_REGEX = Pattern.compile("^VACUUM");
    private static final Pattern DISCARD_ALL_REGEX = Pattern.compile("^DISCARD ALL");
    private static final Pattern ALTER_TYPE_ADD_VALUE_REGEX = Pattern.compile("^ALTER TYPE( .*)? ADD VALUE");

    private static final StatementType COPY = new StatementType();

    public DuckParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 3);
    }

    @Override
    protected char getAlternativeStringLiteralQuote() {
        return '$';
    }

    @Override
    protected ParsedSqlStatement createStatement(PeekingReader reader, Recorder recorder,
                                                 int statementPos, int statementLine, int statementCol, int nonCommentPartPos, int nonCommentPartLine,
                                                 int nonCommentPartCol, StatementType statementType, boolean canExecuteInTransaction, Delimiter delimiter,
                                                 String sql, List<Token> tokens, boolean batchable) throws IOException {
        if (sql.startsWith("ALTER")) {//duckdb do not support alter
            return null;
        }
        return super.createStatement(reader, recorder, statementPos, statementLine, statementCol, nonCommentPartPos,
                nonCommentPartLine, nonCommentPartCol, statementType, canExecuteInTransaction, delimiter, sql, tokens,
                batchable);
    }

    @Override
    protected void adjustBlockDepth(ParserContext context, List<Token> tokens, Token keyword, PeekingReader reader) {
        String keywordText = keyword.getText();

        if (Parser.lastTokenIs(tokens, context.getParensDepth(), "BEGIN") && "ATOMIC".equalsIgnoreCase(keywordText)) {
            context.increaseBlockDepth("ATOMIC");
        }

        if (context.getBlockDepth() > 0 && keywordText.equalsIgnoreCase("END") && "ATOMIC".equals(context.getBlockInitiator())) {
            context.decreaseBlockDepth();
        }
    }


    @Override
    protected StatementType detectStatementType(String simplifiedStatement, ParserContext context, PeekingReader reader) {
        if (COPY_FROM_STDIN_REGEX.matcher(simplifiedStatement).matches()) {
            return COPY;
        }

        return super.detectStatementType(simplifiedStatement, context, reader);
    }

    @Override
    protected Boolean detectCanExecuteInTransaction(String simplifiedStatement, List<Token> keywords) {
        if (CREATE_DATABASE_TABLESPACE_SUBSCRIPTION_REGEX.matcher(simplifiedStatement).matches()
                || ALTER_SYSTEM_REGEX.matcher(simplifiedStatement).matches()
                || CREATE_INDEX_CONCURRENTLY_REGEX.matcher(simplifiedStatement).matches()
                || REINDEX_REGEX.matcher(simplifiedStatement).matches()
                || VACUUM_REGEX.matcher(simplifiedStatement).matches()
                || DISCARD_ALL_REGEX.matcher(simplifiedStatement).matches()) {
            return false;
        }

        boolean isDBVerUnder12 = true;
        try {
            isDBVerUnder12 = !parsingContext.getDatabase().getVersion().isAtLeast("12");
        } catch (Exception e) {
            LOG.debug("Unable to determine database version: " + e.getMessage());
        }

        if (isDBVerUnder12 && ALTER_TYPE_ADD_VALUE_REGEX.matcher(simplifiedStatement).matches()) {
            return false;
        }

        return null;
    }

    @SuppressWarnings("Duplicates")
    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        // dollarQuote is required because in Postgres, literals encased in $$ can be given a label, as in:
        // $label$This is a string literal$label$
        String dollarQuote = (char) reader.read() + reader.readUntilIncluding('$');
        reader.swallowUntilExcluding(dollarQuote);
        reader.swallow(dollarQuote.length());
        return new Token(TokenType.STRING, pos, line, col, null, null, context.getParensDepth());
    }
}
