@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
100 3 4
@================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@@@@@@@
-1 -2 -3
-100 -3 -4
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select 1+100
from tab("/tmp/t1.txt") as t1
@=================

@![result]
@@@@@@@@@@@@@@
101
101
@===================