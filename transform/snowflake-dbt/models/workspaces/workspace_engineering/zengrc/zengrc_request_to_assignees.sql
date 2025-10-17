with source as (select * from {{ ref("zengrc_request_source") }})

select source.request_id, assignees.value['id']::number as assignee_id
from source
inner join lateral flatten(input => try_parse_json(source.assignees)) assignees
