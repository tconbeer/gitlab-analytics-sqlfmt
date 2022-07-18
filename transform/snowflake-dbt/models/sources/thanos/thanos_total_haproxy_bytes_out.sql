with
    source as (select * from {{ source("thanos", "periodic_queries") }}),
    parsed as (

        select
            pq_2.value['metric']['backend']::varchar as metric_backend,
            parse_json(pq_2.value['value'][0]::float)::number::timestamp
            as metric_created_at,
            nullif(pq_2.value['value'][1], 'NaN')::float as metric_value,
            pq_1.value['data']['resultType']::varchar as result_type,
            pq_1.value['status']::varchar as status_type,
            pq_1.this['message']::varchar as message_type,
            pq_1.this['status_code']::number as status_code,
            pq_1.this['success']::boolean as is_success
        from
            source pq,
            lateral flatten(input => pq.jsontext['total_haproxy_bytes_out']) pq_1,
            lateral flatten(
                input
                => pq.jsontext['total_haproxy_bytes_out']['body']['data']['result'],
                outer => true
            ) pq_2
        where result_type is not null and status_type is not null

    )

select *
from parsed
