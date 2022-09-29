{{ config(tags=["mnpi_exception"]) }}

with
    sfdc_opportunity_source as (

        select * from {{ ref("sfdc_opportunity_source") }} where not is_deleted

    ),
    sfdc_account_source as (

        select * from {{ ref("sfdc_account_source") }} where not is_deleted

    ),
    alliance_type as (

        select
            {{
                alliance_type(
                    "fulfillment_partner.account_name",
                    "sfdc_opportunity_source.fulfillment_partner",
                )
            }},
            {{
                alliance_type_short(
                    "fulfillment_partner.account_name",
                    "sfdc_opportunity_source.fulfillment_partner",
                )
            }}
        from sfdc_opportunity_source
        left join
            sfdc_account_source as fulfillment_partner
            on sfdc_opportunity_source.fulfillment_partner
            = fulfillment_partner.account_id
        where sfdc_opportunity_source.fulfillment_partner is not null

    ),
    final as (

        select distinct
            {{ dbt_utils.surrogate_key(["alliance_type"]) }} as dim_alliance_type_id,
            alliance_type as alliance_type_name,
            alliance_type_short as alliance_type_short_name
        from alliance_type

        union all

        select
            md5('-1') as dim_alliance_type_id,
            'Missing alliance_type_name' as alliance_type_name,
            'Missing alliance_type_short_name' as alliance_type_short_name

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@jpeguero",
            created_date="2021-04-07",
            updated_date="2021-09-15",
        )
    }}
