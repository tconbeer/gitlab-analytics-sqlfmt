with source as (select * from {{ ref("zengrc_request_source") }})

select source.request_id, mapped_controls.value['id']::number as control_id
from source
inner join
    lateral flatten(input => try_parse_json(source.mapped_controls)) mapped_controls
