@![sql]
@@@@@@@@@@@@@@@
select ROWS(".*")
from tab("/tmp/t2.txt")
@=====================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@
10 20 30 40
3 4 5 6
@==================

@![result]
@@@@@@@@
10 20 30 40
3 4 5 6
@==================
