@![sql]
@@@@@@@@@@@@@@@
select *
from tab("/tmp/t.txt", ",", 1)
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1,2,3,4
2,2,3,4
3,2,3,4
@==================

@![result]
@@@@@@@@@@
2 2 3 4
3 2 3 4
@==================

@![save]
@!path:/tmp/sql111.txt
@@@@@@@@@@
@==================
