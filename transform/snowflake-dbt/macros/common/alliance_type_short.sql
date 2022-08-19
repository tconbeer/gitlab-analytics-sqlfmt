{%- macro alliance_type_short(fulfillment_partner_name, fulfillment_partner) -%}

case
    when lower({{ fulfillment_partner_name }}) like '%google%'
    then 'GCP'
    when lower({{ fulfillment_partner_name }}) like any ('%aws%', '%amazon%')
    then 'AWS'
    when lower({{ fulfillment_partner_name }}) like '%ibm (oem)%'
    then 'IBM'
    when {{ fulfillment_partner }} is not null
    then 'Non-Alliance'
end as alliance_type_short

{%- endmacro -%}
