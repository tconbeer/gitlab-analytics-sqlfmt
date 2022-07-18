with source as (select * from {{ ref("zengrc_control_source") }})

select source.control_id, mapped_objective.value['id']::number as objective_id
from source
inner join
    lateral flatten(input => try_parse_json(source.mapped_objectives)) mapped_objective
