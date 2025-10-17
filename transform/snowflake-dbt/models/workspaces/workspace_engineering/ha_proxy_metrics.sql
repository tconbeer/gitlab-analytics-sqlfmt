with
    source as (select * from {{ ref("thanos_total_haproxy_bytes_out") }}),

    report as (

        select
            metric_backend as backend,
            metric_created_at as recorded_at,
            case
                when regexp_like(metric_backend, '.*https_git$')
                then 'HTTPs Git Data Transfer'
                when regexp_like(metric_backend, '.*registry$')
                then 'Registry Data Transfer'
                when regexp_like(metric_backend, '.*api$')
                then 'API Data Transfer'
                when regexp_like(metric_backend, '.*web$')
                then 'Web Data Transfer'
                when regexp_like(metric_backend, '.*pages_https?$')
                then 'Pages Data Transfer'
                when regexp_like(metric_backend, '.*websockets$')
                then 'WebSockets Data Transfer'
                when regexp_like(metric_backend, '.*ssh$')
                then 'SSH Data Transfer'
                else 'TBD'
            end as backend_category,
            metric_value as egress_bytes,
            metric_value / (1000 * 1000 * 1000) as egress_gigabytes,
            metric_value / (1024 * 1024 * 1024) as egress_gibibytes
        from source
        -- The first data loads did not include the backend aggregation.
        where metric_backend is not null

    )

select *
from report
