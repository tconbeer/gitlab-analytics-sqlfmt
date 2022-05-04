with
    actuals_cogs_opex as (select * from {{ ref("netsuite_actuals_cogs_opex") }}),
    actuals_income as (select * from {{ ref("netsuite_actuals_income") }}),
    combined as (select * from actuals_cogs_opex union all select * from actuals_income)

select *
from combined
