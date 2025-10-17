with
    source as (

        select *
        from {{ source("engineering", "commit_stats") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['backendCoverage']::float as backend_coverage,
            data_by_row['backendCoverageAbsolute']::number as backend_coverage_absolute,
            data_by_row['backendCoverageTotal']::number as backend_coverage_total,
            data_by_row['commitDate']::date as commit_date,
            data_by_row['jestCoverage']::float as jest_coverage,
            data_by_row['jestCoverageAbsolute']::number as jest_coverage_absolute,
            data_by_row['jestCoverageTotal']::number as jest_coverage_total,
            data_by_row['karmaCoverage']::float as karma_coverage,
            data_by_row['karmaCoverageAbsolute']::number as karma_coverage_absolute,
            data_by_row['karmaCoverageTotal']::number as karma_coverage_total
        from intermediate

    )

select *
from renamed
