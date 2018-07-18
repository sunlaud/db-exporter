package io.github.sunlaud.dbexporter.table;

import com.google.common.collect.Iterables;
import lombok.Data;
import lombok.SneakyThrows;

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

public class TableExporter {
    private javax.sql.DataSource ds;

    public TableExporter(DataSource ds) {
        this.ds = ds;
    }

    @SneakyThrows
    public void export(String exportSql, Set<String> excludedColumns, OutputStream output) {
        try(Connection connection = ds.getConnection();
            PreparedStatement pst = connection.prepareStatement(exportSql);
            ResultSet rs = pst.executeQuery()) {

            Table table = Table.fromQueryResult(rs.getMetaData(), excludedColumns);
            String columnNamesChunk = table.getColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.joining(","));
            PrintStream formattableOut = new PrintStream(output);
            while (rs.next()) {
                String valuesChunk = table.getColumns().stream()
                        .map(column -> column.getValueForInsertFromRow(rs))
                        .collect(Collectors.joining(","));
                formattableOut.format("insert into %s(%s) values(%s);%n", table.getName(), columnNamesChunk, valuesChunk);
            }
            output.flush();
        }
    }

    @Data
    private static class Table {
        private final String name;
        private final Collection<Column> columns;

        public Table(Collection<Column> columns) {
            Set<String> tables = columns.stream().map(Column::getTable).collect(Collectors.toSet());
            checkArgument(tables.size() == 1, "Expected columns from single table, but found from multiple: %s", tables);
            this.name = Iterables.getOnlyElement(tables);
            this.columns = columns;
        }

        @SneakyThrows
        public static Table fromQueryResult(ResultSetMetaData metaData, Set<String> excludedColumns) {
            int columnCount = metaData.getColumnCount();
            List<Column> columns = new ArrayList<>(columnCount);

            for (int i = 1; i <= columnCount; i++) {
                String name = metaData.getColumnName(i);
                if (!excludedColumns.contains(name.toLowerCase()) && !excludedColumns.contains(name.toUpperCase())) {
                    String className = metaData.getColumnClassName(i);
                    String table = metaData.getTableName(i);
                    columns.add(new Column(i, table, name, className));
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

        public Column(int num, String table, String name, String javaClassName) {
            this.num = num;
            this.table = table;
            this.name = name;
            this.javaClassName = javaClassName;
            switch (javaClassName) {
                case "java.sql.Date":
                case "java.lang.String":
                    valueFormatter = value -> "'" + value.toString() + "'";
                    break;
                default:
                    valueFormatter = value -> value.toString();
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
