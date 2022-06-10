with
    source as (select * from {{ source("sheetload", "marketing_kpi_benchmarks") }}),
    renamed as (

        select
            "Goal_Date"::date as goal_date,
            nullif("MQL_Goal", '')::float as mql_goal,
            "Goal_Version"::varchar as goal_version,
            nullif("NetNewOpp_Goal", '')::float as net_new_opp_goal,
            nullif("IACV_Large_Target", '')::float as iacv_large_target,
            nullif("IACV_MM_Target", '')::float as iacv_mm_target,
            nullif("IACV_SMB_Target", '')::float as iacv_smb_target,
            nullif("SAO_Large_Target", '')::float as sao_large_target,
            nullif("SAO_MM_Target", '')::float as sao_mm_target,
            nullif("SAO_SMB_Target", '')::float as sao_smb_target,
            nullif("LandedPipe_Large_Target", '')::float as landed_pipe_large_target,
            nullif("LandedPipe_MM_Target", '')::float as landed_pipe_mm_target,
            nullif("LandedPipe_SMB_Target", '')::float as landed_pipe_smb_target,
            nullif(
                "ClosedWonIACV_Large_Target", ''
            )::float as closed_won_iacv_large_target,
            nullif("ClosedWonIACV_MM_Target", '')::float as closed_won_iacv_mm_target,
            nullif("ClosedWonIACV_SMB_Target", '')::float as closed_won_iacv_smb_target,
            nullif("MQL_Large_Goal", '')::float as mql_large_goal,
            nullif("MQL_MM_Goal", '')::float as mql_mm_goal,
            nullif("MQL_SMB_Goal", '')::float as mql_smb_goal,
            nullif(
                "MQL_To_SAO_Conversion_Large", ''
            )::float as mql_to_sao_conversion_large,
            nullif("MQL_To_SAO_Conversion_MM", '')::float as mql_to_sao_conversion_mm,
            nullif("MQL_To_SAO_Conversion_SMB", '')::float as mql_to_sao_conversion_smb,
            nullif("SDR_SAO_Large_Target", '')::float as sdr_sao_large_target,
            nullif("SDR_SAO_MM_Target", '')::float as sdr_sao_mm_target,
            nullif("SDR_SAO_SMB_Target", '')::float as sdr_sao_smb_target,
            nullif("SDR_IACV_Large_Target", '')::float as sdr_iacv_large_target,
            nullif("SDR_IACV_MM_Target", '')::float as sdr_iacv_mm_target,
            nullif("SDR_IACV_SMB_Target", '')::float as sdr_iacv_smb_target,
            nullif("ASP_Large_Target", '')::float as asp_large_target,
            nullif("ASP_MM_Target", '')::float as asp_mm_target,
            nullif("ASP_SMB_Target", '')::float as asp_smb_target,
            nullif("PipeCreated_Large_Target", '')::float as pipecreated_large_target,
            nullif("PipeCreated_MM_Target", '')::float as pipecreated_mm_target,
            nullif("PipeCreated_SMB_Target", '')::float as pipecreated_smb_target,
            nullif(
                "SDR_PipeCreated_Large_Target", ''
            )::float as sdr_pipecreated_large_target,
            nullif("SDR_PipeCreated_MM_Target", '')::float as sdr_pipecreated_mm_target,
            nullif(
                "SDR_PipeCreated_SMB_Target", ''
            )::float as sdr_pipecreated_smb_target,
            nullif("ExpectedPipe_Large_Target", '')::float as expectedpipe_large_target,
            nullif("ExpectedPipe_MM_Target", '')::float as expectedpipe_mm_target,
            nullif("ExpectedPipe_SMB_Target", '')::float as expectedpipe_smb_target,
            nullif(
                "SDR_ExpectedPipe_Large_Target", ''
            )::float as sdr_expectedpipe_large_target,
            nullif(
                "SDR_ExpectedPipe_MM_Target", ''
            )::float as sdr_expectedpipe_mm_target,
            nullif(
                "SDR_ExpectedPipe_SMB_Target", ''
            )::float as sdr_expectedpipe_smb_target


        from source

    )

select *
from renamed
