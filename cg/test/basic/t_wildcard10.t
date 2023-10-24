@![sql]
@@@@@@@@@@@@@@@
select t.COLUMNS("2")
from tab("/tmp/t2.txt") as t
@=====================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@
2 3 a
4 5 b
@==================

@![result]
@@@@@@@@
2
@==================
