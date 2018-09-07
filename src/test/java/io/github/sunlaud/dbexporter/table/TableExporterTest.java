package io.github.sunlaud.dbexporter.table;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TableExporterTest {



    @Test
    void exportsTableData() {
        //GIVEN
        HikariDataSource ds = new HikariDataSource(new HikariConfig("/datasource.properties"));
        TableExporter sut = new TableExporter(ds);
        String exportSql = "select e.* from emp e inner join dept d on e.deptno = d.deptno where d.dname in ('ACCOUNTING', 'OPERATIONS')";
        Set<String> excludedColumns = Collections.singleton("deptno");

        //WHEN
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        sut.export(exportSql, excludedColumns, output, "EMP");

        //THEN
        String actual = output.toString();
//        assertThat(actual).hasLineCount(2);
        String expected =
                "insert into EMP(EMPNO,ENAME,JOB,HIREDATE) values (7839,NULL,'PRESIDENT','1981-11-17');\n" +
                "insert into EMP(EMPNO,ENAME,JOB,HIREDATE) values (7566,'JONES','MANAGER','1981-04-02');\n";
        assertEquals(expected, actual);
    }
}