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
    and date(start_time) = date;

--
--
--
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
    and date(start_time) = date;

--
--
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
            and date(start_time) = date
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
            and date(start_time) = date
    ) as long_trip;

--
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