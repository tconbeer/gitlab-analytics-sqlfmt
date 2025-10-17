with
    source as (

        select *
        from {{ ref("gitlab_dotcom_user_credit_card_validations_dedupe_source") }}

    ),
    renamed as (

        select
            user_id::number as user_id,
            credit_card_validated_at::timestamp as credit_card_validated_at
        from source

    )

select *
from renamed
