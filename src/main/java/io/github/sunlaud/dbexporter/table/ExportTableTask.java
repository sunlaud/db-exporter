package io.github.sunlaud.dbexporter.table;

import lombok.Data;

import java.util.Collections;
import java.util.Set;

@Data
public class ExportTableTask {
    private final String sql;
    private final String filename;
    private final Set<String> excludedColumns;

    public ExportTableTask(String sql, String filename, Set<String> excludedColumns) {
        this.sql = sql;
        this.filename = filename;
        this.excludedColumns = excludedColumns;
    }

    public ExportTableTask(String sql, String filename) {
        this(sql, filename, Collections.emptySet());
    }
}
