@![sql]
@@@@@@@@@@@@@@@
select $1, $2
from tab("/tmp/t.txt")
format title=true, border="|", column(1)="red";
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
2 3 4 5
3 4 5 6
10 1 1 1
@==================

@![result]
@@@@@@@@@@
-----------------------------------
|$0              |$1              |
-----------------------------------
|1               |2               |
|2               |3               |
|3               |4               |
|10              |1               |
-----------------------------------
@==================
