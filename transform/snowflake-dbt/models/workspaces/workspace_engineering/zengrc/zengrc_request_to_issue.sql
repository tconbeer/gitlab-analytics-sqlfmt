with source as (select * from {{ ref("zengrc_request_source") }})

select source.request_id, mapped_issues.value['id']::number as issue_id
from source
inner join lateral flatten(input => try_parse_json(source.mapped_issues)) mapped_issues
