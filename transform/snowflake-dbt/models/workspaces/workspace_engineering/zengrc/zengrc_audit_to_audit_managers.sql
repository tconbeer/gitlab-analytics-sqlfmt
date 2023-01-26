with source as (select * from {{ ref("zengrc_audit_source") }})

select source.audit_id, audut_manager.value['id']::number as audit_manager_id
from source
inner join lateral flatten(input => try_parse_json(source.audit_managers)) audut_manager
