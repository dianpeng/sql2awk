@![sql]
@@@@@@@@@@@@@@@
select t1.COLUMNS("2"), t2.COLUMNS("4")
from tab("/tmp/t2.txt") as t1,
     tab("/tmp/t3.txt") as t2
@=====================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@
2 3 a
4 5 b
@==================

@![table]
@!name:/tmp/t3.txt
@@@@@@@@
2 3 a
4 5 b
@==================

@![result]
@@@@@@@@
2
2 4
  4
@==================

@![save]
@!path:/tmp/xx.sql
@@@@@@@@
@==================
