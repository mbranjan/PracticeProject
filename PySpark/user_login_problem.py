from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Enlyft_User_Login")
         .master("local[*]")
         .getOrCreate())

df = spark.read.option("header", "true").csv("resources/user_login.txt")

df.createOrReplaceTempView("tbl")

query = """ 
    with CTE as (
    select *, 
    cast(substring(loginTime,1,2) as int) as start_hour, 
    case when cast(substring(loginTime,4,5) as int) <= 30 then 0 else 30 end as start_minute,
    case when cast(substring(loginTime,4,5) as int) <= 30 then start_hour else start_hour + 1 end as end_hour,
    case when cast(substring(loginTime,4,5) as int) <= 30 then 30 else 0 end as end_minute 
    from tbl
),
CTE2 as (
    select user,
    concat( start_hour , ":", start_minute, "-", end_hour, ":",end_minute) as login_window,
    concat("Session starting at " , lpad(start_hour,2,0) , ":" , lpad(start_minute,2,0)) as Sessions
    from CTE
)
select Sessions,collect_list(distinct user) as Users from cte2 group by login_window,Sessions
"""

spark.sql(query).show(truncate=False)
