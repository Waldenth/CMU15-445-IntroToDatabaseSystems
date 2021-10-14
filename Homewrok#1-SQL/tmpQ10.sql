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