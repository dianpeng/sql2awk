@![sql]
@@@@@@@@@@@@@@@
select ($1+$2)*$3 - 4
from tab("/tmp/t.txt")
where $1 > 3 or $1 < 2
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
7
5
@==================
