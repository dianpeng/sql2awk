@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1, "A BC", abcdefg,Holala
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select $2
from csv("/tmp/t1.txt")
@==================

@![result]
@@@@@@@@@@@@@@
A BC
@===================
