# Sql2Awk

Transpile your SQL into AWK

# Brief

This little binary will translate your SQL, only query statement, into AWK script

# Features

- Nearly all SQL query features are supported
  - Join
    - Currently, only natural join is supported
  - Aggregation
    - Min
    - Max
    - Avg
    - Sum
    - Count
    - *Percentile*
    - *Histogram*
  - Group by
  - Order by
    - ASC
    - DESC
  - Distinct
    - Allow select distinct to deduple the output tuple
  - Limit
    - Specify limitation of output as Limit #
  - No schema is needed
    - use field index to reference value, dynamically typped

- Pure awk/gawk
  - No other runtime binary is needed for execution
  - Order by/Percentile requires GAWK (asort/asorti function)

- Extension to SQL
  - Special Aggregation Functions
    - Percentile
      - Calculate the percentile of column, for example getting median number
    - Histogram
      - Calculate the histogram of certain column

  - Rewrite
    - For each table been selected, one can use rewrite keyword to *rewrite* column of table

  - Format
    - Allow fine grained format of the output in terminal. Like color the output for better visibility

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

# Rewrite
select *
from
  tab("sample1.txt") as t1
  rewrite
    when $1 > 0 then set $1 = "Positive";
    when $2 < 0 then set $2 = "Negative";
  end,

  tab("sample2.txt") as t2
  rewrite
    when $1 % 2 == 0 then set $1 = "Even";
    when $2 % 2 != 0 then set $2 = "Odd";
  end

# Format
## print the title bar
select *
from tab("sample1.txt")
format title=true, border="|";

## print the second column in red
select *
from tab("sample1.txt")
format title=true, border="|", column(1)="red";

```

# Status

Currently, it is still under active development and testing. Bugs are expected
and you are more than welcomed to submit an issue.
