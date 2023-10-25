@![sql]
@@@@@@@@@@@@@@@
select *
from tab("/tmp/t.txt")
where $1 like "a_c["
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
3 4 5 6
10 1 1 1
a b c d
axc[ 1 1 1
axcd 10 10 20
axd 10 20 30
@==================

@![result]
@@@@@@@@@@
axc[ 1 1 1
@==================
