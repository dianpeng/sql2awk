@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
100 3 4
@================

@![sql]
@!goawk=disable
@@@@@@@@@@@@@@@@@@
select *
from tab("/tmp/t1.txt")
order by $1, $2
@==================

@![save]
@!path:/tmp/sql.txt
@@@@@@@@@@@@@@
@===================
