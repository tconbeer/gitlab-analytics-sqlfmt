with source as (select * from {{ ref("zengrc_assessment_source") }})

select source.assessment_id, assessors.value['id']::number as assessor_id
from source
inner join lateral flatten(input => try_parse_json(source.assessors)) assessors
