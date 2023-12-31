@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
1 2 3
100 3 4
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select 
  $1 > 1 ?
    (
      ($2 > -1000 ? 
        ($3 > -100000000000 ? 1000 : -100000) : -1000000
      )
    )
  : ($1+$2+$3 > 1000 ? -8888 : -7777)

from tab("/tmp/t1.txt") as t1
@=================

@![result]
@@@@@@@@@@@@@@
-7777
1000
@===================
