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
select (t2.$1 + t2.$2 + t2.$3)
from tab("/tmp/t1.txt") as t1,
     tab("/tmp/t2.txt") as t2
@==================

@![result]
@@@@@@@@@@@@@@
-6
-107
-6
-107
@===================
