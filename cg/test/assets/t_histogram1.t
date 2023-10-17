@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
1 2 3
@================

@![sql]
@!goawk=disable
@@@@@@@@@@@@@@@@@@
select histogram($1, 0, 200, 10)
from tab("/tmp/t1.txt")
@==================

@![save]
@!path:/tmp/sql3.txt
@@@@@@@@@@@@@@
@===================
