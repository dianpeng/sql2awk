@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
100 3 4
9 3 4
8 3 4
7 3 4
10 3 4
2 3 4
4 3 4
1 2 3
@================

@![sql]
@!goawk=disable
@@@@@@@@@@@@@@@@@@
select *
from tab("/tmp/t1.txt")
order by $1, $2
@==================

@![result]
@!sort:none
@@@@@@@@@@@@@@
1 2 3
2 3 4
4 3 4
7 3 4
8 3 4
9 3 4
10 3 4
100 3 4
@===================