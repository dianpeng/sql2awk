@![sql]
@@@@@@@@@@@@@@@
select $2, $3, $4
from tab("/tmp/t.txt")
where $1 in (200, 300) or $1 in (1, 2, 3)
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
3 4 5 6
10 1          1 1
@==================

@![result]
@@@@@@@@@@
2 3 4
3 4 5
4 5 6
@==================
