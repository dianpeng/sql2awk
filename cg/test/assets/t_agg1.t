@![sql]
@@@@@@@@@@@@@@@
select avg($1)
from tab("/tmp/t.txt")
having sum($1) == 16
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
3 4 5 6
10 1 1 1
@==================

@![result]
@@@@@@@@@@
4
@==================
