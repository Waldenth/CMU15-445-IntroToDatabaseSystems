with event_cnt (events, cnt) as (
    select
        events,
        count(distinct date) as cnt
    from
        weather
    group by
        events
)
select
    weather.events,
    round(1.0 * count(distinct trip.id) / event_cnt.cnt, 4) as avg_num
from
    trip,
    station,
    weather,
    event_cnt
where
    start_station_id = station_id
    and date(trip.start_time) = weather.date
    and station.zip_code = weather.zip_code
    and weather.events = event_cnt.events
group by
    weather.events
order by
    avg_num desc,
    weather.events asc;

with event_cnt (events, cnt) as (
    select
        events,
        count(distinct date) as cnt
    from
        weather
    group by
        events
)
select
    w.events,
    round(1.0 * count(distinct t.id) / event_cnt.cnt, 4) as avg_num
from
    trip as t,
    station as s,
    weather as w,
    event_cnt
where
    t.start_station_id = s.station_id
    and date(t.start_time) = w.date
    and s.zip_code = w.zip_code
    and w.events = event_cnt.events
group by
    w.events
order by
    avg_num desc,
    w.events asc