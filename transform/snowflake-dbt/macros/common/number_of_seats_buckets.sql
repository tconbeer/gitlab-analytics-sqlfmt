{%- macro number_of_seats_buckets(number_of_seats) -%}

    case
        when {{ number_of_seats }} <= 0
        then '[00] <= 0'
        when {{ number_of_seats }} between 1 and 5
        then '[01] 1-5'
        when {{ number_of_seats }} between 6 and 7
        then '[02] 6-7'
        when {{ number_of_seats }} between 8 and 10
        then '[03] 8-10'
        when {{ number_of_seats }} between 11 and 15
        then '[04] 11-15'
        when {{ number_of_seats }} between 16 and 50
        then '[05] 16-50'
        when {{ number_of_seats }} between 51 and 100
        then '[06] 51-100'
        when {{ number_of_seats }} between 101 and 200
        then '[07] 101-200'
        when {{ number_of_seats }} between 201 and 500
        then '[08] 201-500'
        when {{ number_of_seats }} between 501 and 1000
        then '[09] 501-1,000'
        when {{ number_of_seats }} >= 1001
        then '[10] 1,001+'
    end

{%- endmacro -%}
