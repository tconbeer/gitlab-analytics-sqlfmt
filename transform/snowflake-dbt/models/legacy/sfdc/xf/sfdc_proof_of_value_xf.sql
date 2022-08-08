with
    sfdc_pov as (select * from {{ ref("sfdc_proof_of_value") }}),
    sfdc_users as (select * from {{ ref("sfdc_users_xf") }}),
    joined as (

        select
            sfdc_pov.*,
            owner.name as pov_owner_name,
            solarch.name as solution_architect_name,
            tam.name as technical_account_manager_name
        from sfdc_pov
        left join sfdc_users as owner on sfdc_pov.pov_owner_id = owner.user_id
        left join
            sfdc_users as solarch on sfdc_pov.solutions_architect_id = solarch.user_id
        left join
            sfdc_users as tam on sfdc_pov.technical_account_manager_id = tam.user_id

    )

select *
from joined
