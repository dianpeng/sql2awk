@![sql]
@@@@@@@@@@@@@@@
select *
from tab("/tmp/t.txt")
where $1 > 3 and $1 < 5
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
@==================
