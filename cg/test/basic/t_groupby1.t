@![sql]
@@@@@@@@@@@@@@@
select $1, min($2)
from tab("/tmp/t.txt")
group by $1
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
1 4 5 6
1 1 1 1
@==================

@![result]
@@@@@@@@@@
1 1
2 3
@==================
