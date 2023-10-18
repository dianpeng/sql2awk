@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
100 3 4
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
@!order:none
@@@@@@@@@@@@@@
1 2 3
100 3 4
@===================
