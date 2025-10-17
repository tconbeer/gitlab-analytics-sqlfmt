{%- macro alliance_type(fulfillment_partner_name, fulfillment_partner) -%}

    case
        when lower({{ fulfillment_partner_name }}) like '%google%'
        then 'Google Cloud'
        when lower({{ fulfillment_partner_name }}) like any ('%aws%', '%amazon%')
        then 'Amazon Web Services'
        when lower({{ fulfillment_partner_name }}) like '%ibm (oem)%'
        then 'IBM (OEM)'
        when {{ fulfillment_partner }} is not null
        then 'Non-Alliance Partners'
    end as alliance_type

{%- endmacro -%}
