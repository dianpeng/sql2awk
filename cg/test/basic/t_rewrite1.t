@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
100 3 4
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select *
from tab("/tmp/t1.txt")
     rewrite
       when $1 == 1 then set $1=20
     end
@==================

@![result]
@@@@@@@@@@@@@@
20 2 3
100 3 4
@===================
