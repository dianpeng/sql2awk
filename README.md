# Sql2Awk

This little binary will translate your SQL, only query statement, into AWK script.
You can directly run the generated AWK script or combine the AWK with other
traditional linux command line tools to form your own anlytical tools.

# Features

- Query
  - Join
    - Currently, only natural join is supported
  - Aggregation Function
    - Min
    - Max
    - Avg
    - Sum
    - Count
      - Count(*) is allowed
    - *Percentile*
      - Require GAWK function
    - *Histogram*
  - Group by
  - Order by
    - Asc/Desc order both supports
    - Notes, to support this feature, the generated code will have to use GAWK function *asort/asorti*
  - Distinct
  - Limit
  - No schema is needed
  - Star/Wildcard matching
  - Like/Regex/Pattern
    - like/not like operator are supported
      - By translating them into regexp
    - match/not match operator are supported
      - Powered by AWK/GAWK's regex

- Just AWK/GAWK code
  - No other runtime tools/library/binary is needed for execution
  - Order by/Percentile requires GAWK (asort/asorti function)

- Advanced Features
  - Special Aggregation Functions
    - Percentile
      - Calculate the percentile of column, for example getting median number
    - Histogram
      - Calculate the histogram of certain column

  - Wildcard matching
    - Match a specific set of columns by specifying a regex expression
    - Match a specific line by specifying a regex expression

  - Rewrite
    - For each table been selected, one can use rewrite keyword to *rewrite* column of table

  - Format
    - Allow fine grained format of the output in terminal. Like color the output for better visibility

- File Format
  - Tab/Tabular
    - Using tab/tabular type inside of *from* clause indicates that the file is parsed as delimter separated file, by default it just uses space delimited file
      - ``` select * from tab("sample1.txt") ```
      - ``` select * from tab("sample1.txt", ":", 1, 200) ```
        - selecting a every fields separated by ":", starting from line 1 until line 200

    - Using csv/xsv type inside of *from* clause indicates that the file is parsed as csv file, this will *correctly* handle quoted string, but it is not performant
      - ``` select * from csv("sample1.txt") ```
      - ``` select * from csv("sample1.txt", ",", 1, 200) ```
        - selecting a every fields via CSV syntax separated by ",", starting from line 1 until line 200

- Scheme
  - No scheme is needed, use $N to reference the N'th field
  - $1 represents first field, $2 second, ...
  - $0 represents the full line
  - $FN represents the field count after parsing

- Builtin Functions
  - Type
    - is_decimal
    - is_integer
    - is_string
    - is_empty
    - type
    - cast

  - String
    - string_length
    - string_to_lower
    - string_to_upper
    - string_substr
    - string_index
    - string_include
    - string_format
      - sprintf
  - Math
    - math_cos
    - math_sin
    - math_sqrt
    - math_exp
    - math_int
    - math_log
    - math_atan2

  - Bit
    - bit_and
    - bit_or
    - bit_not
    - bit_lshift
    - bit_rshift

  - Base64
    - base64_decode
    - base64_encode

# Caveats

  - Type is limited
    - AWK/GAWK can only support numerical type and string type
    - NULL is missing

  - CSV is not support properly
    - Currently due to limitation of AWK/GAWK, it cannot support CSV/TSV
    - Other approach is to convert CSV into tabular data and handle it inside of awk
    - We will have pure AWK implementation for CSV/JSON parsing, but not performant

# Example

````
----------------------------------------------------------------
-- Basic SQL features
----------------------------------------------------------------

-- select all the fields from the file, delimited by space
select * from tab("sample.txt")

-- distinct
select distinct * from tab("sample.txt")

-- select 1st first field
select $1 from tab("sample.txt")

-- aggregation

select count($1) from tab("sample.txt")
select avg($1) from tab("sample.txt")
select sum($1) from tab("sample.txt")
select max($1) from tab("sample.txt")
select min($1) from tab("sample.txt")

-- aggregation with group by
select count($1), $2+100
from tab("sample.txt")
group by $2

-- filter
select *
from tab("sample.txt")
where $1 > 10 && $2 != 20

-- having
select min($1)
from tab("sample.txt")
group by $2
having max($3) > 10

-- order by, require gawk to run
select *
from tab("sample.txt")
order by $2

-- special aggregation
select percentile($1, 10) # 10% high value
from tab("sample.txt")

select histogram($1, 1, 20, 5) # histgoram distribution with min/max/# of bins
from tab("sample.txt")

-- join
select t1.$1, t2.$2
from tab("sample1.txt") as t1,
     tab("sample2.txt") as t2
where t1.$2 == t2.$1

----------------------------------------------------------------
-- Advanced SQL features
----------------------------------------------------------------

-- row/column wildcard filtering

-- filtering all lines in tabluar file with specified regex pattern
select ROWS("[0-9][0-9]")
from tab("sample1.txt")

-- filtering all columns that is composed by specified regex pattern
select COLUMNS("[a-z][a-z]+")
from tab("sample1.txt")

-- filtering certain table's ROW or COLUMNS
select t1.COLUMNS("abc*"), t2.ROWS("[0-9]+")
from tab("sample1.txt") as t1,
     tab("sample2.txt") as t2

-- calling builtin functions
select *
from tab("sample1.txt")
where is_string($2)

-- Rewrite
-- rewrite the table value before filtering, joining etc ... rewrite phase
-- happened at very early stage
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

-- Format
-- print the title bar
select *
from tab("sample1.txt")
format title=true, border="|";

-- print the second column in red
select *
from tab("sample1.txt")
format title=true, border="|", column(1)="red";

````

# Status
Currently, it is still under active development and testing. Bugs are expected
and you are more than welcomed to submit an issue.
