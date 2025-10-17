{{ simple_cte([("dim_usage_ping_metric", "dim_usage_ping_metric")]) }}

select *
from dim_usage_ping_metric
where time_frame = 'none'
