@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
2 1 3
3 1 2
@================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@@@@@@@
10 2 3
20 3 2
@================

@![sql]
@!goawk=disable
@@@@@@@@@@@@@@@@@@
select *
from tab("/tmp/t1.txt") as t1,
     tab("/tmp/t2.txt") as t2
order by t1.$1
@==================

@![result]
@!order:none
@@@@@@@@@@@@@@
2 1 3 10 2 3
2 1 3 20 3 2
3 1 2 10 2 3
3 1 2 20 3 2
@===================
