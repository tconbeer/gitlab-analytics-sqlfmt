with
    sfdc_contact as (

        select
            {{
                dbt_utils.star(
                    from=ref("sfdc_contact"),
                    except=["CONTACT_EMAIL", "CONTACT_NAME"],
                )
            }}
        from {{ ref("sfdc_contact") }}

    ),
    sfdc_record_type as (select * from {{ ref("sfdc_record_type") }}),
    joined as (

        select
            sfdc_contact.*,
            sfdc_record_type.business_process_id,
            sfdc_record_type.record_type_label,
            sfdc_record_type.record_type_description,
            sfdc_record_type.record_type_modifying_object_type
        from sfdc_contact
        left join
            sfdc_record_type
            on sfdc_contact.record_type_id = sfdc_record_type.record_type_id

    )

select *
from joined
