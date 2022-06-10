with source as (select * from {{ ref("zengrc_assessment_source") }})

select source.assessment_id, audits.value['id']::number as audit_id
from source
inner join lateral flatten(input => try_parse_json(source.mapped_audits)) audits
