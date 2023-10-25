@![table]
@!name:/tmp/t1.txt
@@@@@@@@@@@@@@
helloworld xxx
yyy zzz
@================

@![sql]
@@@@@@@@@@@@@@@@@@
select string_to_upper($1)
from tab("/tmp/t1.txt")
where regexp_is_match($2, "...")
@==================

@![result]
@@@@@@@@@@@@@@
HELLOWORLD
YYY
@===================
