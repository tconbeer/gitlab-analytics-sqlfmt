with base as (select * from {{ ref("driveload_ssa_coverage_fitted_curves_source") }})

select *
from base
