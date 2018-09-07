package io.github.sunlaud.dbexporter.table;

import lombok.Data;

import java.util.Collections;
import java.util.Set;

@Data
public class ExportTableTask {
    private final String targetTableName;
    private final String filename;
    private final String sql;
    private final Set<String> excludedColumns;

    public ExportTableTask(String targetTableName, String filename, String sql, Set<String> excludedColumns) {
        this.targetTableName = targetTableName;
        this.sql = sql;
        this.filename = filename;
        this.excludedColumns = excludedColumns;
    }

    public ExportTableTask(String targetTableName, String filename, String sql) {
        this(targetTableName, filename, sql, Collections.emptySet());
    }
}
