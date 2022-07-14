with
    qualtrics_distribution as (

        select *
        from {{ ref("qualtrics_distribution") }}
        qualify
            row_number() OVER (partition by distribution_id order by uploaded_at desc)
            = 1

    )

select *
from qualtrics_distribution
