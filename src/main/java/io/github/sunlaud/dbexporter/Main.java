package io.github.sunlaud.dbexporter;

import com.google.common.collect.Sets;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.sunlaud.dbexporter.table.ExportTableTask;
import io.github.sunlaud.dbexporter.table.TableExporter;
import io.github.sunlaud.dbexporter.table.TemplatedExportTableTask;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import javax.sql.DataSource;

@Slf4j
public class Main {

    public static void main(String[] args) {
        new Main().run();
    }

    @SneakyThrows
    private void run() {
        HikariConfig hikariConfig = new HikariConfig("/datasource.properties");
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setReadOnly(true);

        DataSource ds = new HikariDataSource(hikariConfig);
        HashSet<String> globalIgnoredColumns = Sets.newHashSet("last_modified_by", "created_by");
        Collection<ExportTableTask> tablesToExport = Arrays.asList(
                new ExportTableTask("a", "0.a.sql", "select * from a;")
        );

        Path outputDir = Files.createTempDirectory("db-export-");

        TableExporter tableExporter = new TableExporter(ds);
        System.out.println("Exporting " + tablesToExport.size() + " tables to " + outputDir);
        for (ExportTableTask tableTask : tablesToExport) {
            File outFile = outputDir.resolve(tableTask.getFilename()).toFile();
            outFile.createNewFile();
            try (FileOutputStream fos = new FileOutputStream(outFile, false);
                 OutputStream output = new BufferedOutputStream(fos)
            ) {
                log.info("Exporting table: " + tableTask);
                tableExporter.export(tableTask.getSql(), tableTask.getExcludedColumns(), output, tableTask.getTargetTableName());
            }

        }
        log.info("Export finished. See results in " + outputDir);
    }

}
