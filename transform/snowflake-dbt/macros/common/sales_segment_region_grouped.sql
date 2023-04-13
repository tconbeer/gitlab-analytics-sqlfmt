{%- macro sales_segment_region_grouped(segment, sales_geo, sales_region) -%}

    case
        when
            {{ segment }} in ('Large', 'PubSec')
            and {{ sales_geo }} = 'AMER'
            and lower({{ sales_region }}) = 'west'
        then 'US West'
        when
            {{ segment }} in ('Large', 'PubSec')
            and {{ sales_geo }} in ('AMER', 'LATAM')
            and lower({{ sales_region }}) in ('east', 'latam')
        then 'US East'
        when
            {{ segment }} in ('Large', 'PubSec')
            and {{ sales_geo }} in ('APAC', 'PubSec', 'EMEA', 'Global')
        then {{ sales_geo }}
        when {{ segment }} in ('Large', 'PubSec') and {{ sales_region }} = 'PubSec'
        then 'PubSec'
        when
            {{ segment }} in ('Large', 'PubSec')
            and {{ sales_geo }}
            not in ('West', 'East', 'APAC', 'PubSec', 'EMEA', 'Global')
        then 'Large Other'
        when {{ segment }} not in ('Large', 'PubSec')
        then {{ segment }}
        else 'Missing segment_region_grouped'
    end

{%- endmacro -%}
