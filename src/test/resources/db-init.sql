create table dept(
  deptno numeric,
  dname  varchar(14),
  loc    varchar(13),
  constraint pk_dept primary key ( deptno )
);

create table emp(
  empno    numeric,
  ename    varchar(10),
  job      varchar(9),
  hiredate date,
  deptno   numeric,
  constraint pk_emp primary key ( empno ),
  constraint fk_deptno foreign key ( deptno ) references dept ( deptno )
);

insert into dept values( 10, 'ACCOUNTING', 'NEW YORK' );
insert into dept values( 20, 'RESEARCH', 'DALLAS' );
insert into dept values( 30, 'SALES', 'CHICAGO' );
insert into dept values( 40, 'OPERATIONS', 'BOSTON' );

insert into emp(empno, ename, job, hiredate, deptno) values(7839,NULL,'PRESIDENT','1981-11-17',10);
insert into emp(empno, ename, job, hiredate, deptno) values(7698,'BLAKE','MANAGER','1981-05-01',20);
insert into emp(empno, ename, job, hiredate, deptno) values(7782,'CLARK','MANAGER','1981-09-09',30);
insert into emp(empno, ename, job, hiredate, deptno) values(7566,'JONES','MANAGER','1981-04-02',40);