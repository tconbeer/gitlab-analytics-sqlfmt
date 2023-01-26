with source as (select * from {{ source("driveload", "ssa_coverage_fitted_curves") }})

select
    key_agg_day::varchar as key_agg_day,
    agg_key_name::varchar as agg_key_name,
    agg_key_value::varchar as agg_key_value,
    close_day_of_fiscal_quarter_normalised::number
    as close_day_of_fiscal_quarter_normalised,
    bookings_linearity::number as bookings_linearity,
    open_1plus_net_arr_coverage::number as open_1plus_net_arr_coverage,
    open_3plus_net_arr_coverage::number as open_3plus_net_arr_coverage,
    open_4plus_net_arr_coverage::number as open_4plus_net_arr_coverage,
    rq_plus_1_open_1plus_net_arr_coverage::number
    as rq_plus_1_open_1plus_net_arr_coverage,
    rq_plus_1_open_3plus_net_arr_coverage::number
    as rq_plus_1_open_3plus_net_arr_coverage,
    rq_plus_1_open_4plus_net_arr_coverage::number
    as rq_plus_1_open_4plus_net_arr_coverage,
    rq_plus_2_open_1plus_net_arr_coverage::number
    as rq_plus_2_open_1plus_net_arr_coverage,
    rq_plus_2_open_3plus_net_arr_coverage::number
    as rq_plus_2_open_3plus_net_arr_coverage,
    rq_plus_2_open_4plus_net_arr_coverage::number
    as rq_plus_2_open_4plus_net_arr_coverage

from source
