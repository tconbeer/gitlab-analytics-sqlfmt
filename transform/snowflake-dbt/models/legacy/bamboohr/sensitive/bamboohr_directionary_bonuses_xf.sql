with
    bamboohr_discretionary_bonuses as (

        select * from {{ ref("bamboohr_discretionary_bonuses") }}
    )

select employee_id, bonus_date, count(*) as total_discretionary_bonuses
from bamboohr_discretionary_bonuses
group by 1, 2
