with
    source as (

        select

            visitor_id as visitor_id,
            cookie_id as cookie_id,
            event_date as event_date,
            modified_date as modified_date,
            ip_address as ip_address,
            experiment_id as experiment_id,
            experiment_name as experiment_name,
            variation_id as variation_id,
            variation_name as variation_name,
            abtest_user_id as abtest_user_id,
            is_deleted as is_deleted,
            _created_date as _created_date,
            _modified_date as _modified_date,
            _deleted_date as _deleted_date

        from {{ source("bizible", "biz_customer_ab_tests") }}

    )

select *
from source
