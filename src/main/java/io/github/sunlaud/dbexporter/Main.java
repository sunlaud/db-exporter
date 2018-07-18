package io.github.sunlaud.dbexporter;

import com.google.common.io.Files;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.sunlaud.dbexporter.table.ExportTableTask;
import io.github.sunlaud.dbexporter.table.TableExporter;
import lombok.SneakyThrows;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;

public class Main {

    public static void main(String[] args) {
        new Main().run();
    }

    @SneakyThrows
    private void run() {
        HikariDataSource ds = new HikariDataSource(new HikariConfig("/datasource.properties"));
        ds.setReadOnly(true);

        Collection<ExportTableTask> tablesToExport = Arrays.asList(
                new ExportTableTask("select * from emp", "9.9.emp.sql"),
                new ExportTableTask("select * from dept", "9.8.dept.sql")
        );

        File outputDir = Files.createTempDir();
        TableExporter tableExporter = new TableExporter(ds);
        for (ExportTableTask tableTask : tablesToExport) {
            File outFile = outputDir.toPath().resolve(tableTask.getFilename()).toFile();
            outFile.createNewFile();
            OutputStream output = new BufferedOutputStream(new FileOutputStream(outFile, false));
            tableExporter.export(tableTask.getSql(), tableTask.getExcludedColumns(), output);
        }
        System.out.println("Export finished. See results in " + outputDir);
    }

}
