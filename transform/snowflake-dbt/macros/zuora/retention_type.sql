{%- macro retention_type(original_mrr, new_mrr) -%}

    case
        when {{ new_mrr }} = 0 and {{ original_mrr }} > 0
        then 'Cancelled'
        when {{ new_mrr }} < {{ original_mrr }} and {{ new_mrr }} > 0
        then 'Downgraded'
        when {{ new_mrr }} > {{ original_mrr }}
        then 'Upgraded'
        when {{ new_mrr }} = {{ original_mrr }}
        then 'Maintained'
        else 'Other'
    end as retention_type

{%- endmacro -%}
