package io.github.sunlaud.dbexporter.table;

import java.util.Collections;
import java.util.Set;

public class TemplatedExportTableTask extends ExportTableTask {
    public TemplatedExportTableTask(String targetTableName, String filename, String sql, Set<String> excludedColumns) {
        super(targetTableName, filename, sql.replace("%TABLENAME%", targetTableName), excludedColumns);
    }

    public TemplatedExportTableTask(String targetTableName, String filename, String sql) {
        this(targetTableName, filename, sql, Collections.emptySet());
    }
}
