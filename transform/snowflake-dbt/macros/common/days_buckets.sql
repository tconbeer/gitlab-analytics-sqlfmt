{%- macro days_buckets(day_field) -%}

    case
        when {{ day_field }} between 0 and 6
        then '[01] 0-6 Days'
        when {{ day_field }} between 7 and 14
        then '[02] 7-14 Days'
        when {{ day_field }} between 15 and 21
        then '[03] 15-21 Days'
        when {{ day_field }} between 22 and 30
        then '[04] 22-30 Days'
        when {{ day_field }} between 31 and 60
        then '[05] 31-60 Days'
        when {{ day_field }} between 61 and 90
        then '[06] 61-90 Days'
        when {{ day_field }} between 91 and 120
        then '[07] 91-120 Days'
        when {{ day_field }} between 121 and 180
        then '[08] 121-180 Days'
        when {{ day_field }} between 181 and 365
        then '[09] 181-365 Days'
        when {{ day_field }} between 366 and 730
        then '[10] 1-2 Years'
        when {{ day_field }} between 731 and 1095
        then '[11] 2-3 Years'
        when {{ day_field }} > 1095
        then '[12] 3+ Years'
    end

{%- endmacro -%}
