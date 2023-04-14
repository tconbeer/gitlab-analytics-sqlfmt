{%- macro arr_buckets(arr) -%}

    case
        when {{ arr }} < 0
        then '[00] < 0'
        when {{ arr }} between 0 and 250
        then '[01] 0-250'
        when {{ arr }} between 250 and 500
        then '[02] 250-500'
        when {{ arr }} between 500 and 1000
        then '[03] 500-1K'
        when {{ arr }} between 1000 and 2500
        then '[04] 1K-2.5K'
        when {{ arr }} between 2500 and 5000
        then '[05] 2.5K-5K'
        when {{ arr }} between 5000 and 10000
        then '[06] 5K-10K'
        when {{ arr }} between 10000 and 25000
        then '[07] 10K-25K'
        when {{ arr }} between 25000 and 50000
        then '[08] 25K-50K'
        when {{ arr }} between 50000 and 100000
        then '[09] 50K-100K'
        when {{ arr }} between 100000 and 500000
        then '[10] 100K-500K'
        when {{ arr }} between 500000 and 1000000
        then '[11] 500K-1M'
        when {{ arr }} > 1000000
        then '[12] 1M+'
    end

{%- endmacro -%}
