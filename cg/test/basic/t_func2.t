@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
helloworld xxx
yyy zzz
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select to_upper($1)
from tab("/tmp/t1.txt")
@==================

@![result]
@@@@@@@@@@@@@@
HELLOWORLD
YYY
@===================
