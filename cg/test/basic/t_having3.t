@![sql]
@@@@@@@@@@@@@@@
select $1, $2, min($3)
from tab("/tmp/t.txt")
group by $1, $2
having (min($3) + max($4)) == 6
@=================

@![table]
@!name:/tmp/t.txt
@@@@@@@@@@
1 2 3 4
1 2 2 5
1 2 1 3 
2 2 10 4
2 2 4 3
@==================

@![result]
@@@@@@@@@@
1 2 1
@==================
