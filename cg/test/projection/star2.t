@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
100 3 4
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select t.*
from tab("/tmp/t1.txt") as t
@==================

@![result]
@@@@@@@@@@@@@@
1 2 3
100 3 4
@===================
