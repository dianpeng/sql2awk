@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1
@================

@![table]
@!name:/tmp/t2.txt
@@@@@@@@@@@@@@
2
@================


@![table]
@!name:/tmp/t3.txt
@@@@@@@@@@@@@@
3
@================

@![table]
@!name:/tmp/t4.txt
@@@@@@@@@@@@@@
4
@================

@![table]
@!name:/tmp/t5.txt
@@@@@@@@@@@@@@
5
@================

@![table]
@!name:/tmp/t6.txt
@@@@@@@@@@@@@@
1
1
1
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select t1.*, t2.*, t3.$1*100, t4.*, t5.$1 - 100
from tab("/tmp/t1.txt") as t1,
     tab("/tmp/t2.txt") as t2,
     tab("/tmp/t3.txt") as t3,
     tab("/tmp/t4.txt") as t4,
     tab("/tmp/t5.txt") as t5,
     tab("/tmp/t6.txt") as t6
@==================

@![result]
@@@@@@@@@@@@@@
 1                2                300              4                -95
 1                2                300              4                -95
 1                2                300              4                -95
@===================
