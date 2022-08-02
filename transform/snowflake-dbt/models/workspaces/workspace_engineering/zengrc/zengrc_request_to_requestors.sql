with source as (select * from {{ ref("zengrc_request_source") }})

select source.request_id, requestors.value['id']::number as requestor_id
from source
inner join lateral flatten(input => try_parse_json(source.requestors)) requestors
