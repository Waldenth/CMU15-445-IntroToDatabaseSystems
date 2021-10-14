## CMU15-445 Homework#1-SQL (2018 Fall)

家庭作业：高级SQL

[参考博客](https://blog.csdn.net/weixin_44520881/article/details/111235025)

### 环境配置

安装sqlite3，解压并导入数据库文件，在数据库目录下使用作业数据库，测试

```sql
$ sqlite3 bike_sharing.db
SQLite version 3.11.0
Enter ".help" for usage hints.
sqlite> .tables
station  trip     weather
```

问题案例Q1

Count the number of cities. The purpose of this query is to make sure that the formatting of your output matches exactly the formatting of our auto- grading script. 计算城市数量，排除重复

**Details:** Print the number of cities (eliminating duplicates).

**Answer**: Here's the correct SQL query and expected output:

```SQL
sqlite> select count(distinct(city)) from station;
5
```

### 问题

正式SQL作业

![schema](https://gitee.com/waldenth/blogimage/raw/master/img/schema.png)



weather table的主键是联合主键，要求date和zip_code组合起来没有重复

#### Q2 Warm up

Count the number of stations in each city. 计算每个城市的站台数量

**Details:** Print city name and number of stations. Sort by number of stations (increasing), and break ties by city name (increasing).打印城市名和对应站台数量组成的表，表项先以站台数递增，再按照城市名递增排序

**thinking:** 

根据city聚合(提取聚合函数的数据要使用group by)，使用group by city后，表项按照不同的city每个station项分别聚集，从按city聚集后(即现在的station表成为了以city划分开的一个个子集)的station表中选择city，count(station_id)(聚合函数count计算station_id数，每个子集中的station_id数)，最后asc排序

```sql
sqlite> select city, count(station_id) as sta_cnt  from station group by city order by sta_cnt asc, city asc;
Palo Alto|5
Mountain View|7
Redwood City|7
San Jose|16
San Francisco|35
```

#### Q3 [10 POINTS] (Q3_POPULAR_CITY):

Find the percentage of trips in each city. A trip belongs to a city as long as its start station or end station is in the city. For example, if a trip started from station A in city P and ended in station B in city Q, then the trip belongs to both city P and city Q. If P equals to Q, the trip is only counted once.

查找每个城市的出行百分比。只要起点站或终点站在城市内，旅行就属于城市。例如，如果行程从P市的a站开始，到Q市的B站结束，则该行程同时属于P市和Q市。如果P等于Q，则该行程只计算一次。

查询每个城市名，以及其所占trip总数的比例，以比列降序，城市名升序排列

**Details:** Print city name and ratio between the number of trips that belong to that city against the total number of trips (a decimal between 0-1, round to decimal places using ). Sort by ratio (decreasing), and break ties by city name (increasing). `four``ROUND()`

`(C) as trip_cnt `  给表C 取一个别名`trip_cnt`

```sql
select city, ratio
from city_trip_cnt, trip_cnt
order by ratio DESC, city ASC;

select city, (A) as ratio
from (B) as city_trip_cnt, (C) as trip_cnt
order by ratio DESC, city ASC;

A: # 以每个城市的旅行数除以旅行总数保留4位小数的比例数据组成的列
round(city_trip_cnt.cnt * 1.0 / trip_cnt.cnt, 4)

B: 
# trip表和station表笛卡尔积 中 station_id为trip表的起终点站的项(条件连接)
# 并根据city将这些项划分成子集
# 从子集中选取city和不同trip.id的数量组成新表
select city, count(distinct(id)) as cnt from trip,station 
where station_id=start_station_id or station_id = end_station_id
group by city

C:
# 旅行线路的总数,直接成表对象
select count(*) as cnt from trip 

```

最终答案

```sql
select city, (round(city_trip_cnt.cnt * 1.0 / trip_cnt.cnt, 4)) as ratio
from (
select city, count(distinct(id)) as cnt from trip,station 
where station_id=start_station_id or station_id = end_station_id
group by city
) as city_trip_cnt, (select count(*) as cnt from trip ) as trip_cnt
order by ratio DESC, city ASC;

San Francisco|0.9011
San Jose|0.0566
Mountain View|0.0278
Palo Alto|0.0109
Redwood City|0.0052
```

#### Q4 [15 POINTS] (Q4_MOST_POPULAR_STATION):

For each city, find the most popular station in that city. "Popular" means that the station has the highest count of visits. As above, either starting a trip or finishing a trip at a station, the trip is counted as one "visit" to that station. The trip is only counted once if the start station and the end station are the same.

对于每个城市，找到该城市最受欢迎的车站。“受欢迎”意味着该车站的访问次数最多。如上所述，车站无论是在旅行线路的起点还是终点，行程都被视为对该车站的一次“访问”。如果起点站和终点站相同，则仅计算一次行程。

**Details:** For each station, print city name, most popular station name and its visit count. Sort by city name, ascending. 对于每个站点，打印城市名称、最受欢迎的站点名称及其访问次数。按城市名称升序排序。

```sql
select visit.city, visit.station_name, visit.cnt
from (A) as visit
where visit.cnt = (	select max(max_visit.cnt)
					from (A) as max_visit
					where max_visit.city = visit.city) 
					#max_visit是visit的一个副本
					#对每个visit.city，在max_visit表中city相同的项中找到最大cnt值(city中车站的最大访问量)
					#父子查询的先后顺序不绝对,这里子查询依赖父查询,父查询先开始
order by city asc;

A: 
-- trip和station连接，取车站id是旅行线路起始或终点站的连接项
-- 根据sta_id划分子集
-- 从每个子集中选择sta.city, sta_id, sta_name, trip数量(及该站为起始终点站的trip数量) as cnt
select city, station_id, station_name, count(id) as cnt
from trip, station
where station_id = start_station_id or station_id = end_station_id
group by station_id

select visit.city,visit.station_name,visit.cnt
from(
select city, station_id, station_name, count(id) as cnt
from trip, station
where station_id = start_station_id or station_id = end_station_id
group by station_id) as visit
where visit.cnt =(
select max(max_visit.cnt) from (
    select city, station_id, station_name, count(id) as cnt 
    from trip, station 
    where station_id = start_station_id or station_id = end_station_id 
    group by station_id) as max_visit 
    where max_visit.city = visit.city)
order by city asc;


Mountain View|Mountain View Caltrain Station|12735
Palo Alto|Palo Alto Caltrain Station|3534
Redwood City|Redwood City Caltrain Station|2654
San Francisco|San Francisco Caltrain (Townsend at 4th)|111738
San Jose|San Jose Diridon Caltrain Station|18782
```

使用`with as`将表`A`先生成逻辑更为清晰：

```sql
with visit(station_id, station_name, city, cnt) as (
	select station_id, station_name, city, count(id) as cnt 
	from trip, station 
	where station_id = start_station_id or station_id = end_station_id 
	group by station_id)

select visit.city, visit.station_name, visit.cnt 
from visit 
where visit.cnt = (	select max(cnt) 
					from visit as max_visit 
					where max_visit.city = visit.city) 
order by city;
```

#### Q5 [15 POINTS] (Q5_DAYS_MOST_BIKE_UTILIZATION):

Find the top 10 days that have the highest average bike utilization. For simplicity, we only consider trips that use bikes with id <= 100. The average bike utilization on date D is calculated as the sum of the durations of all the trips that happened on date D divided by the total number of bikes with id <= 100, which is a constant. If a trip overlaps with date D, but starts before date D or ends after date D, then only the interval that overlaps with date D (from 0:00 to 24:00) will be counted when calculating the average bike utilization of date D. And we only calculate the average bike utilization for the date that has been either a start or an end date of a trip. You can assume that no trip has negative time (i.e., for all trips, start time <= end time).

找出平均自行车使用率最高的前10天。为了简单起见，我们只考虑使用ID为<=100的自行车。日期D上的平均自行车利用率计算为日期D上发生的所有行程的持续时间之和除以id≤100的自行车总数，这是一个常数。如果行程与日期D重叠，但在日期D之前开始或在日期D之后结束，则在计算日期D的平均自行车利用率时，仅计算与日期D重叠的时间间隔（从0:00到24:00）。我们仅计算行程开始或结束日期的平均自行车利用率。您可以假设没有行程具有负时间（即，对于所有行程，开始时间<=结束时间）。

**Details:** For the dates with the top 10 average duration, print the date and the average bike duration on that date (in seconds, round to four decimal places using the ROUND() function). Sort by the average duration, decreasing. Please refer to the updated note before Q1 when calculating the duration of a trip.

详细信息：对于前10个平均持续时间的日期，打印日期和该日期的平均自行车持续时间（以秒为单位，使用round（）函数四舍五入到小数点后四位）。按平均持续时间排序，递减。计算行程持续时间时，请参考第一季度之前更新的注释。 

**Hint:** All timestamps are stored as text after loaded from csv in sqlite. You can use datetime(timestamp string) to get the timestamp out of the string and date(timestamp string) to get the date out of the string. You may also find the funtion strftime() helpful in computing the duration between two timestamps.

从sqlite中的csv加载后，所有时间戳都存储为文本。可以使用datetime（时间戳字符串）从字符串中获取时间戳，使用date（时间戳字符串）从字符串中获取日期。你还可能发现函数strftime（）有助于计算两个时间戳之间的持续时间。

`date(timestring, modifier, modifier, ...)	以 YYYY-MM-DD 格式返回日期。`

`strftime(format, timestring, modifier, modifier, ...)	这将根据第一个参数指定的格式字符串返回格式化的日期`

`datetime(timestring, modifier, modifier, ...)	以 YYYY-MM-DD HH:MM:SS 格式返回`

```sql
with dates as (
    select
        date(start_time) as tdate
    from
        trip
    union
    select
        date(end_time) as tdate
    from
        trip
)#把trip表中开始结束日期合并为一列别名tdate，存于dates表中，即tdate每一个日期都有trip开始或者结束
select 
	tdate,
    round(
        sum(
            strftime('%s',min(datetime(end_time), datetime(tdate, '+1 day'))) 
            --tdate转换成带hms的时间是一天的零点，+1day则到了第二天零点也就是这一天的结束
            --由于后文where保证了end_time时间上大于tadte零点，因此end_time必属于tdate该天或之后的天
            --则在tdate一日中,本旅行的结尾时间是是两者中的最小值(end属于该天则是end，属于后面的天则是该天结束时间)
            - 
            strftime('%s', max(datetime(start_time), datetime(tdate)))
        ) * 1.0 / (
            select
                count(distinct(bike_id))
            from
                trip
            where
                bike_id <= 100
        ),
        4
    ) as avg_duration
    #每个日期上发生的所有行程的持续时间之和除以id≤100的自行车总数得到的利用率avg_duration
from
    trip,
    dates # 从trip表和dates表的笛卡尔积中取行
where
    bike_id <= 100 
    and datetime(start_time) < datetime(tdate, '+1 day')
    and datetime(end_time) > datetime(tdate) 
    #取的行要求bike_id<=100，旅行开始日期小于tdate+1零点(即开始日期<=tdate) 且结束日期大于tdate零点
group by
    tdate
order by
    avg_duration desc
limit
    10;
```

```bash
		           |--------本天--------|
		                     		endtime
		           ...................................->          			
					starttime
      <-................................		 
___________________|___________________|___________________
                   tdate            tdate+1
```

#### Q6 [10 POINTS] (Q6_OVERLAPPING_TRIPS)

One of the possible data-entry errors is to record a bike as being used in two different trips, at the same time. Thus, we want to spot pairs of overlapping intervals (start time, end time). To keep the output manageable, we ask you to do this check for bikes with id between 100 and 200 (both inclusive). Note: Assume that no trip has negative time, i.e., for all trips, start time <= end time.

一个可能的数据输入错误是记录一辆自行车同时在两次不同的旅行中使用。因此，我们希望发现成对的重叠间隔（开始时间、结束时间）。为了保持输出的可管理性，我们要求您对id介于100和200（包括100和200）之间的自行车进行检查。注：假设没有行程具有负时间，即对于所有行程，开始时间<=结束时间。

**Details:** For each conflict (a pair of conflict trips), print the bike id, former trip id, former start time, former end time, latter trip id, latter start time, latter end time. Sort by bike id (increasing), break ties with former trip id (increasing) and then latter trip id (increasing).

详细信息：对于每个冲突（一对冲突行程），打印自行车id、前行程id、前开始时间、前结束时间、后行程id、后开始时间、后结束时间。按自行车id（增）排序，先断开前一个行程id（增），然后断开后一个行程id（增）。

**Hint:** 

(1) Report each conflict pair only once, so that former trip id < latter trip id.

(2) We give you the (otherwise tricky) condition for conflicts: start1 < end2 AND end1 > start2

每个冲突对只报告一次，以便前一个旅行id<后一个旅行id。
我们为您提供了冲突的条件（否则很棘手）：start1<end2且end1>start2

```sql
select 
	former_trip.bike_id,
	former_trip.id,
	former_trip.start_time,
	former_trip.end_time,
	latter_trip.id,
	latter_trip.start_time,
	latter_trip.end_time
from
	trip as former_trip,
	trip as latter_trip
where
	former_trip.bike_id>=100
	and former_trip.bike_id<=200
	and former_trip.bike_id=latter_trip.bike_id
	and former_trip.start_time < latter_trip.end_time
	and former_trip.end_time > latter_trip.start_time
	and former_trip.id<latter_trip.id
order by
	former_trip.bike_id ASC,
	former_trip.id ASC,
	latter_trip.id ASC;
```

#### Q7 [10 POINTS] (Q7_MULTI_CITY_BIKES)

Find all the bikes that have been to more than one city. A bike has been to a city as long as the start station or end station in one of its trips is in that city.
查找所有去过不止一个城市的自行车。一个自行车可视为去过一个城市，只要它的起点站或终点站在该城市。

**Details:** For each bike that has been to more than one city, print the bike id and the number of cities it has been to. Sort by the number of cities (decreasing), then bike id (increasing).
对于每辆去过多个城市的自行车，打印自行车id，去过的城市数量，根据城市数量降序排序再根据车ID增序排序

```sql
select 
	bike_id,
	count(distinct(city)) as cnt
from
	trip,station
where
	station_id=start_station_id
	or
	station_id=end_station_id
group by
	bike_id
having
	cnt>1
order by
	cnt DESC,
	bike_id asc;
```

#### Q8 [10 POINTS] (Q8_BIKE_POPULARITY_BY_WEATHER)

Find what is the average number of trips made per day on each type of weather day. The type of weather on a day is specified by weather.events, such as ‘Rain’, ‘Fog’ and so on. For simplicity, we consider all days that does not have a weather event (weather.events = ‘\N’) as a single type of weather. Here a trip belongs to a date only if its start time is on that date. We use the weather at the starting position of that trip as its weather type as well. There are also ‘Rain’ and ‘rain’ in weather.events. For simplicity, we consider them as different types of weathers. When counting the total number of days for a weather, we consider a weather happened on a date as long as it happened in at least one region on that date.

找出每种天气类型每天的平均出行次数。一天的天气类型由weather.events指定，如“雨”、“雾”等。为了简单起见，我们考虑所有没有天气事件的天气（weather.events = ‘\N’）作为一种天气类型。这里，只有当旅行的开始时间在某个日期时，旅行才属于该日期。我们也使用旅行开始位置的天气作为其天气类型。weather.events中也有“Rain”和“rain”。为了简单起见，我们认为它们是不同类型的天气。当计算一天的总天数时，我们认为在一个日期发生的天气，只要发生在那个天的至少一个时间段中。(weather.date和trip.date相同)

**Details:** Print the name of the weather and the average number of trips made per day on that type of weather (round to four decimal places using ROUND()). Sort by the average number of trips (decreasing), then weather name (increasing).

打印天气名称，该天气类型下每天的平均出行次数（使用round（）四舍五入到小数点后四位）。按平均旅行次数排序（减少），然后按天气名称排序（增加）。

```
坑来了，DISTINCT 有带括号和没括号的两种用法

1、 如果跟其它函数结合使用，那么只会使用小括号内的参数
2、 否则，那么 DISTINCT 关键字后的所有列都是参数
```

```sql
with event_cnt (events, cnt) as (
    select
        events,
        count(distinct date) as cnt
    from
        weather
    group by
        events
)
--获取临时表event_cnt,由weather表中根据events划分子集后每个子集中的events,唯一日期数
select
    weather.events,
    round(1.0 * count(distinct trip.id) / event_cnt.cnt, 4) as avg_num
from
    trip,
    station,
    weather,
    event_cnt
where
	--旅行开始位置的天气为旅行天气类型确定station位置,进而zip_code确定weather
    trip.start_station_id = station.station_id -- 这里由于字段不同,trip. station.可以不加
    and date(trip.start_time) = weather.date
    and station.zip_code = weather.zip_code
    and weather.events = event_cnt.events
group by
    weather.events
order by
    avg_num desc,
    weather.events asc;
```

#### Q9 [10 POINTS] (Q9_TEMPERATURE_SHORTER_TRIPS)

A short trip is a trip whose duration is <= 60 seconds. Compute the average temperature that a short trip starts versus the average temperature that a non-short trip starts. We use weather.mean_temp on the date of the start time as the Temperature measurement.

短行程是持续时间小于等于60秒的行程。计算短行程开始时的平均温度与非短行程开始时的平均温度。我们使用开始时间当天的`weather.mean_temp`作为温度测量值。

**Details:** Print the average temperature that a short trip starts and the average temperature that a non-short trip starts. (on the same row, and both round to four decimal places using ROUND()) Please refer to the updated note before Q1 when calculating the duration of a trip.

打印短行程开始的平均温度和非短行程开始的平均温度。（在同一行，使用round（）将两位小数点后四位四舍五入）计算行程时，请参阅Q1之前更新的注释。

**连接trip、station、weater表的时候要使用date 和 zip_code 和station这几个字段来连接**

查询短行程

```sql
select
	round(1.0 * sum(mean_temp)/count(*),4 ) as temp
from 
	trip,
	station,
	weather
where
	strftime("%s",end_time) - strftime("%s",start_time) <= 60
	and start_station_id=station_id
	and station.zip_code=weather.zip_code --这里两表字段相同不能省略表名了
	and date(start_time)= weather.date --这里不是函数date,是列名date
```

查询长行程

```sql
select
    round(1.0 * sum(mean_temp) / count(*), 4) as temp
from
    trip,
    station,
    weather
where
    strftime("%s", end_time) - strftime("%s", start_time) > 60
    and start_station_id = station_id
    and station.zip_code = weather.zip_code
    and date(start_time) = weather.date
```

合并两表查询即可

```sql
select
    short_trip.temp,
    long_trip.temp
from
    (
        select
            round(1.0 * sum(mean_temp) / count(*), 4) as temp
        from
            trip,
            station,
            weather
        where
            strftime("%s", end_time) - strftime("%s", start_time) <= 60
            and start_station_id = station_id
            and station.zip_code = weather.zip_code
            and date(start_time) = weather.date
    ) as short_trip,
    (
        select
            round(1.0 * sum(mean_temp) / count(*), 4) as temp
        from
            trip,
            station,
            weather
        where
            strftime("%s", end_time) - strftime("%s", start_time) > 60
            and start_station_id = station_id
            and station.zip_code = weather.zip_code
            and date(start_time) = weather.date
    ) as long_trip;
```

注意这里short_trip,long_trip仍然进行了笛卡尔积，但是两表都只有1列1行，所有所以最后还是1行的结果

#### Q10 [15 POINTS] (Q10_RIDING_IN_STORM)

For each zip code that has experienced ‘Rain-Thunderstorm’ weather, find the station that has the most number of trips in that zip code under the storm weather. For simplicity, we only consider the start time of a trip when deciding the station and the weather for that trip.

对于每个经历过`Rain-Thunderstorm`天气的邮政编码`zip code`，请查找在该`zip code`下在风暴天气下出行次数最多的车站。为了简单起见，我们只考虑一次旅行的开始时间来决定火车站和那次旅行的天气。

**Details:** Print the zip code that has experienced the ‘Rain-Thunderstorm’ weather, the name of the station that has the most number of trips under the strom weather in that zip code, and the total number of trips that station has under the storm weather. Sort by the zip code (increasing). You do not need to print the zip code that has experienced ‘Rain-Thunderstorm’ weather but no trip happens on any storm day in that zip code.

打印经历过`Rain-Thunderstorm`天气的邮政编码`zip code`、该`zip code`中在风暴天气下出行次数最多的车站的名称、车站在风暴天气下的总出行次数。

按`zip code`排序（递增）。您不需要打印经历过`Rain-Thunderstorm`天气的`zip code`，但在该`zip code`中的任何风暴日都不会发生旅行。

由于要找最大的，同时要考虑并列的，因此`where`中也需要重复查询`storm_count`，因此抽出来一个`storm_count`。

```sql
select
    zip_code,
    station_name,
    cnt
from
    (A)
where
    cnt = (B)
order by
    zip_code asc;


A: 
-- 在雷雨天下 不同车站在不同zipcode下的旅行总数cnt
with storm_count as (
    select
        s.zip_code,
        s.station_id,
        s.station_name,
        count(*) as cnt --cnt是以zip_code,station_id划分的子集合中满足where的元素个数
    from
        weather as w,
        station as s,
        trip as t
    where
        date(t.start_time) = w.date --旅行日是天气日
        and t.start_station_id = s.station_id -- 旅行始发站是这一站
        and s.zip_code = w.zip_code -- 天气的联合主键中的zipcode和这一站的zipcode相同
    	and events = 'Rain-Thunderstorm' -- 天气是雷雨天
    group by
        s.zip_code,
        s.station_id
) 
B:
--在zip_code和当前的雷雨天zip_code(storm_count.zip_code)相同下,雷雨天不同车站中出行的最大数量
select
    max(max_s_c.cnt)
from
    (A) as max_s_c 
where
    storm_count.zip_code = max_s_c.zip_code 
```

```sql
with storm_count as (
    select
        s.zip_code,
        s.station_id,
        s.station_name,
        count(*) as cnt
    from
        weather as w,
        station as s,
        trip as t
    where
        date(t.start_time) = w.date
        and t.start_station_id = s.station_id
        and s.zip_code = w.zip_code
        and events = 'Rain-Thunderstorm'
    group by
        s.zip_code,
        s.station_id
)
select
    zip_code,
    station_name,
    cnt
from
    storm_count
where
    cnt = (
        select
            max(max_s_c.cnt)
        from
            storm_count as max_s_c
        where
            storm_count.zip_code = max_s_c.zip_code
    )
order by
    zip_code asc;
```

