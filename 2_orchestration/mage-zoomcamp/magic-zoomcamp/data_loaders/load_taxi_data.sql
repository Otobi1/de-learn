-- Docs: https://docs.mage.ai/guides/sql-blocks

select
    tpep_pickup_datetime,
    count(*) row_cnt
from ny_taxi.yellow_taxi_ride
group by 1