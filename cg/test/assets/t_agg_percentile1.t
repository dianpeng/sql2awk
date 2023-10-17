@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
1 2 3
@================

@![sql]
@!goawk=disable
@@@@@@@@@@@@@@@@@@
select percentile($1, 50)
from tab("/tmp/t1.txt")
@==================

@![save]
@!path:/tmp/sql2.txt
@@@@@@@@@@@@@@
@===================
