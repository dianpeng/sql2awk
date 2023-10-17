# Sql2Awk

Transpiling SQL directly to AWK script.

# Brief

This little binary allows user to specify simple SQL statment, currently only
supports query, and it will translate it into AWK code for execution.

# Features

- Nearly all SQL query related feature is supported
  - Join
  - Aggregation
  - Group by
  - Order by
  - Distinct
  - Limit
  - No schema needed, use field index to reference value, dynamically typped

- Special aggregation function
  - Percentile
  - Histogram

- Pure awk/gawk, no other runtime binary is needed for execution
  - Order by/Percentile requires GAWK (asort/asorti function)

# Example

```

# select all the fields from the file, delimited by space
select * from tab("sample.txt")

# distinct
select distinct * from tab("sample.txt")

# select 1st first field
select $1 from tab("sample.txt")

# aggregation

select count($1) from tab("sample.txt")
select avg($1) from tab("sample.txt")
select sum($1) from tab("sample.txt")
select max($1) from tab("sample.txt")
select min($1) from tab("sample.txt")

# aggregation with group by
select count($1), $2+100
from tab("sample.txt")
group by $2

# filter
select *
from tab("sample.txt")
where $1 > 10 && $2 != 20

# having

select min($1)
from tab("sample.txt")
group by $2
having max($3) > 10

# order by, require gawk to run

select *
from tab("sample.txt")
order by $2

# special aggregation

select percentile($1, 10) # 10% high value
from tab("sample.txt")

select histogram($1, 1, 20, 5) # histgoram distribution with min/max/# of bins
from tab("sample.txt")

# join

select t1.$1, t2.$2
from tab("sample1.txt") as t1,
     tab("sample2.txt") as t2
where t1.$2 == t2.$1

# calling builtin function, will add more
select *
from tab("sample1.txt")
where is_string($2)

```

# Status

Bugs are expected for now, will do more tests :)
