with
    sheetload_osat as (select * from {{ ref("sheetload_osat") }}),
    intermediate as (

        select
            date_trunc(month, completed_date) as completed_month,
            sum(satisfaction_score) as osat_score,
            sum(buddy_experience_score) as buddy_experience_score,
            count(*) as total_responses,
            count(
                iff(buddy_experience_score is not null, 1, 0)
            ) as total_buddy_score_responses
        from sheetload_osat
        group by 1

    ),
    rolling_3_month as (

        select
            *,
            sum(osat_score) OVER (
                order by completed_month rows between 2 preceding and current row
            ) as sum_of_rolling_3_month_score,
            sum(total_responses) OVER (
                order by completed_month rows between 2 preceding and current row
            ) as rolling_3_month_respondents,
            sum(buddy_experience_score) OVER (
                order by completed_month rows between 2 preceding and current row
            ) as sum_of_rolling_3_month_buddy_score,
            sum(total_buddy_score_responses) OVER (
                order by completed_month rows between 2 preceding and current row
            ) as rolling_3_month_buddy_respondents
        from intermediate

    ),
    final as (

        select
            *,
            sum_of_rolling_3_month_score
            / rolling_3_month_respondents as rolling_3_month_osat,
            sum_of_rolling_3_month_buddy_score
            / rolling_3_month_buddy_respondents as rolling_3_month_buddy_score
        from rolling_3_month

    )

select *
from final
