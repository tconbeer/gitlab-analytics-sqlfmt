with source as (select * from {{ ref("zengrc_issue_source") }})

select source.issue_id, mapped_audits.value['id']::number as audit_id
from source
inner join lateral flatten(input => try_parse_json(source.mapped_audits)) mapped_audits
