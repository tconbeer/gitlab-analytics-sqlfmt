with source as (select * from {{ ref("zengrc_issue_source") }})

select source.issue_id, mapped_programs.value['id']::number as program_id
from source
inner join
    lateral flatten(input => try_parse_json(source.mapped_programs)) mapped_programs
