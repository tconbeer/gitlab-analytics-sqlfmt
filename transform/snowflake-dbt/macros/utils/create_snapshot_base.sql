{%- macro create_snapshot_base(
    source, primary_key, date_start, date_part, snapshot_id_name
) -%}

with
    date_spine as (

        select distinct date_trunc({{ date_part }}, date_day) as date_actual
        from {{ ref("date_details") }}
        where date_day >= '{{ date_start }}'::date and date_day <= current_date

    ),
    base as (

        select *
        from {{ source }}
        qualify
            row_number() over (
                partition by
                    {{ primary_key }}, date_trunc({{ date_part }}, dbt_valid_from)
                order by dbt_valid_from desc
            )
            = 1

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key([primary_key, "date_actual"]) }} as unique_key,
            dbt_scd_id as {{ snapshot_id_name }},
            date_actual,
            dbt_valid_from as valid_from,
            dbt_valid_to as valid_to,
            iff(dbt_valid_to is null, true, false) as is_currently_valid,
            base.*
        from base
        inner join
            date_spine
            on base.dbt_valid_from::date <= date_spine.date_actual
            and (
                base.dbt_valid_to::date > date_spine.date_actual
                or base.dbt_valid_to is null
            )
        order by 2, 3

    )

{%- endmacro -%}
