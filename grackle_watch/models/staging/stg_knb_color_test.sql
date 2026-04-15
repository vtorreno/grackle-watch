-- stg_knb_color_test.sql
with source as (
    select * from {{ source('grackle_watch_external', 'knb_color_test') }}
),

staged as (
    select
        -- identifiers
        bird,
        experiment,
        batch,
        trial,

        -- experimental conditions
        color_on_left,

        -- outcomes
        is_correct,

        -- temporal
        date as experiment_date,

        -- audit & provenance
        'KNB' as data_source,
        current_timestamp() as dbt_staged_at

    from source
)

select * from staged