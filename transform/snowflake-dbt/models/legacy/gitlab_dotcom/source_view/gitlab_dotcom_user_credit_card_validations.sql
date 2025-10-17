with
    source as (

        select * from {{ ref("gitlab_dotcom_user_credit_card_validations_source") }}

    )

select *
from source
