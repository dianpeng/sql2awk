@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
100 3 4
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select $50
from tab("/tmp/t1.txt")
@==================

@![result]
@@@@@@@@@@@@@@
@===================
