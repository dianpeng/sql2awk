@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1, "A BC", abcdefg,Holala
@================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@@@@@@@
1 2 3 4
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select t1.$2, t2.*
from csv("/tmp/t1.txt") as t1,
     tab("/tmp/t2.txt") as t2
@==================

@![result]
@@@@@@@@@@@@@@
A BC 1 2 3 4
@===================
