@![sql]
@@@@@@@@@@@@@@@
select *
from tab("/tmp/t.txt")
where $1 like "a_c"
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
3 4 5 6
10 1 1 1
a b c d
axc 1 1 1
axd 1 1 2
@==================

@![result]
@@@@@@@@@@
axc 1 1 1
@==================

@![save]
@!path:/tmp/abc1234.sql
@@@@@@@@@@
@==================
