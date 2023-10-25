@![sql]
@@@@@@@@@@@@@@@
select *
from tab("/tmp/t.txt")
where $1 not match "^[0-9]+$"
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
3 4 5 6
10 1 1 1
a b c d
@==================

@![result]
@@@@@@@@@@
a b c d
@==================
