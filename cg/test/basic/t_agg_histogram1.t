@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
1 2 3
@================

@![sql]
@!awk=sys
@@@@@@@@@@@@@@@@@@
select histogram($1, 0, 200, 10)
from tab("/tmp/t1.txt")
@==================

@![result]
@@@@@@@@@@@@@@
!0;2;0;0;0;0;0;0;0;0;0;!0
@===================
