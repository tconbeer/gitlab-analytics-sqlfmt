with
    source as (

        select * from {{ ref("sfdc_proof_of_concept_source") }} where is_deleted = false

    )

select *
from source
