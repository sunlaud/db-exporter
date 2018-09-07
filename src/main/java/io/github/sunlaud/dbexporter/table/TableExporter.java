package io.github.sunlaud.dbexporter.table;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class TableExporter {
    private javax.sql.DataSource ds;

    public TableExporter(DataSource ds) {
        this.ds = ds;
    }

    @SneakyThrows
    public void export(String exportSql, Set<String> excludedColumns, OutputStream output, String targetTableName) {
        try(Connection connection = ds.getConnection();
            PreparedStatement pst = connection.prepareStatement(exportSql)) {
            try (ResultSet rs = pst.executeQuery()) {
                log.info("got results for table {}", targetTableName);
                Table table = Table.fromQueryResult(targetTableName, rs.getMetaData(), excludedColumns);
                String columnNamesChunk = table.getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(","));
                PrintStream formattableOut = new PrintStream(output, true, "UTF-8");
                insertHeader(formattableOut, table);
                while (rs.next()) {
                    String valuesChunk = table.getColumns().stream()
                            .map(column -> column.getValueForInsertFromRow(rs))
                            .collect(Collectors.joining(","));
                    formattableOut.format("insert into %s(%s) values (%s);%n", table.getName(), columnNamesChunk, valuesChunk);
                }
                insertFooter(formattableOut, table);
            }

            output.flush();
        }
    }

    private void insertHeader(PrintStream formattableOut, Table table) {
        String header =
                "--liquibase formatted sql\n" +
                identityInsertChangesetChunk(table, true) +
                "\n--changeset " + generateId(table) + "\n";
        formattableOut.print(header);
    }

    private String identityInsertChangesetChunk(Table table, boolean turnOn) {
        if (!table.hasAutoIncrementColumns) {
            return "";
        }
        return
                "\n--changeset " + generateId(table) + " dbms:mssql runAlways:true\n" +
                "SET IDENTITY_INSERT " + table.getName() + " " + (turnOn ? "ON" : "OFF") + ";\n";
    }

    private void insertFooter(PrintStream formattableOut, Table table) {
        formattableOut.print("\n" + identityInsertChangesetChunk(table, false));
    }

    private String generateId(Table table) {
        return "TEST_DATA:" + table.getName() + "-" + System.nanoTime();
    }

    @Data
    private static class Table {
        private final String name;
        private final Collection<Column> columns;
        private final boolean hasAutoIncrementColumns;

        public Table(Collection<Column> columns) {
            Set<String> tables = columns.stream().map(Column::getTable).collect(Collectors.toSet());
            checkArgument(tables.size() == 1, "Expected columns from single table, but found from multiple: %s", tables);
            this.name = Iterables.getOnlyElement(tables);
            this.columns = columns;
            this.hasAutoIncrementColumns = columns.stream().anyMatch(Column::isAutoincrement);
        }

        @SneakyThrows
        public static Table fromQueryResult(String tableName, ResultSetMetaData metaData, Set<String> excludedColumns) {
            int columnCount = metaData.getColumnCount();
            List<Column> columns = new ArrayList<>(columnCount);

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (!excludedColumns.contains(columnName.toLowerCase()) && !excludedColumns.contains(columnName.toUpperCase())) {
                    String className = metaData.getColumnClassName(i);
//                    String table = metaData.getTableName(i);
//                    checkState(!Strings.isNullOrEmpty(table), "Sadly, but JDBC driver returned empty table name for column '%s'", name);
                    columns.add(new Column(i, tableName, columnName, className, metaData.isAutoIncrement(i)));
                }
            }
            return new Table(columns);
        }
    }

    @Data
    private static class Column {
        private final int num;
        private final String table;
        private final String name;
        private final String javaClassName;
        private final Function<Object, String> valueFormatter;
        //TODO regex compilation
        private final Function<Object, String> BASIC_FORMATTER = value -> (value == null ? "NULL" : value.toString().replaceAll("'", "''").replaceAll("\n", ""));
        private final boolean autoincrement;

        public Column(int num, String table, String name, @NonNull String javaClassName, boolean autoincrement) {
            this.autoincrement = autoincrement;
            checkArgument(!Strings.isNullOrEmpty(table), "table name should be not blank");
            checkArgument(!Strings.isNullOrEmpty(name), "column name should be not blank");
            this.num = num;
            this.table = table;
            this.name = name;
            this.javaClassName = javaClassName;
            switch (javaClassName) {
                case "java.sql.Date":
                case "java.sql.Timestamp":
                case "java.lang.String":
                    valueFormatter = value -> value == null ? BASIC_FORMATTER.apply(value) : "'" + BASIC_FORMATTER.apply(value) + "'";
                    break;
                case "java.lang.Boolean":
                    valueFormatter = value -> value == Boolean.TRUE ? "((1))" : "((0))"; //TODO mssql specific
                    break;
                default:
                    valueFormatter = BASIC_FORMATTER;
                    break;
            }
        }

        @SneakyThrows
        public String getValueForInsertFromRow(ResultSet rs) {
            Object value = rs.getObject(num);
            return valueFormatter.apply(value);
        }
    }
}
