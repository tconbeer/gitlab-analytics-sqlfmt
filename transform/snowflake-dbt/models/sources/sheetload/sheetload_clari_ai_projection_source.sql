with
    source as (select * from {{ source("sheetload", "clari_ai_projection") }}),
    renamed as (

        select
            "Date"::date as projection_date,
            "Projection_$"::number as projection_dollar_amount,
            "EOQ_Closed_$"::number as eoq_closed_dollar_amount,
            "Projection_Error_Percentage"::number as projection_error_percentage,
            "Projection_Lower_Bound_$"::number as projection_lower_bound_dollar_amount,
            "Projection_Upper_Bound_$"::number as projection_upper_bound_dollar_amount,
            "Top_1_Source_of_Error"::varchar as top_one_source_of_error,
            "Top_1_Source_of_Error_$_Diff"::number
            as top_one_source_of_error_dollar_amount_diff,
            "Top_2_Source_of_Error"::varchar as top_two_source_of_error,
            "Top_2_Source_of_Error_$_Diff"::number
            as top_two_source_of_error_dollar_amount_diff,
            "Closed_as_of_Date_$"::number as closed_as_of_date_dollar_amount,
            "Closed_Converted_by_EOQ_$"::number
            as closed_converted_by_eoq_dollar_amount,
            "Projected_Closed_Converted_by_EOQ_$"::number
            as projected_closed_converted_by_eoq_dollar_amount,
            "Closed_Converted_by_EOQ_Percentage"::number
            as closed_converted_by_eoq_percentage,
            "Projected_Closed_Converted_by_EOQ_Percentage"::number
            as projected_closed_converted_by_eoq_percentage,
            "New_as_of_Date_$"::number as new_as_of_date_dollar_amount,
            "New_Converted_by_EOQ_$"::number as new_converted_by_eoq_dollar_amount,
            "Projected_New_Converted_by_EOQ_$"::number
            as projected_new_converted_by_eoq_dollar_amount,
            "New_Converted_by_EOQ_Percentage"::number
            as new_converted_by_eoq_percentage,
            "Projected_New_Converted_by_EOQ_Percentage"::number
            as projected_new_converted_by_eoq_percentage,
            "Best_Case_as_of_Date_$"::number as best_case_as_of_date_dollar_amount,
            "Best_Case_Converted_by_EOQ_$"::number
            as best_case_converted_by_eoq_dollar_amount,
            "Projected_Best_Case_Converted_by_EOQ_$"::number
            as projected_best_case_converted_by_eoq_dollar_amount,
            "Best_Case_Converted_by_EOQ_Percentage"::number
            as best_case_converted_by_eoq_percentage,
            "Projected_Best_Case_Converted_by_EOQ_Percentage"::number
            as projected_best_case_converted_by_eoq_percentage,
            "Commit_as_of_Date_$"::number as commit_as_of_date_dollar_amount,
            "Commit_Converted_by_EOQ_$"::number
            as commit_converted_by_eoq_dollar_amount,
            "Projected_Commit_Converted_by_EOQ_$"::number
            as projected_commit_converted_by_eoq_dollar_amount,
            "Commit_Converted_by_EOQ_Percentage"::number
            as commit_converted_by_eoq_percentage,
            "Projected_Commit_Converted_by_EOQ_Percentage"::number
            as projected_commit_converted_by_eoq_percentage,
            "Omitted_as_of_Date_$"::number as omitted_as_of_date_dollar_amount,
            "Omitted_Converted_by_EOQ_$"::number
            as omitted_converted_by_eoq_dollar_amount,
            "Projected_Omitted_Converted_by_EOQ_$"::number
            as projected_omitted_converted_by_eoq_dollar_amount,
            "Omitted_Converted_by_EOQ_Percentage"::number
            as omitted_converted_by_eoq_percentage,
            "Projected_Omitted_Converted_by_EOQ_Percentage"::number
            as projected_omitted_converted_by_eoq_percentage,
            "Pipeline_as_of_Date_$"::number as pipeline_as_of_date_dollar_amount,
            "Pipeline_Converted_by_EOQ_$"::number
            as pipeline_converted_by_eoq_dollar_amount,
            "Projected_Pipeline_Converted_by_EOQ_$"::number
            as projected_pipeline_converted_by_eoq_dollar_amount,
            "Pipeline_Converted_by_EOQ_Percentage"::number
            as pipeline_converted_by_eoq_percentage,
            "Projected_Pipeline_Converted_by_EOQ_Percentage"::number
            as projected_pipeline_converted_by_eoq_percentage
        from source
    )

select *
from renamed
