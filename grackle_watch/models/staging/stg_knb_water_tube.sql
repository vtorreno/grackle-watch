-- stg_knb_water_tube.sql
with source as (
    select * from {{ source('grackle_watch_external', 'knb_water_tube') }}
),

staged as (
    select
        -- identifiers
        bird,
        experiment,
        batch,
        trial,
        choice_number,

        -- experimental conditions
        tube_on_left,
        choice,
        water_level,

        -- outcomes
        choice_correct,
        extracted_food,

        -- temporal
        date as experiment_date,

        -- audit & provenance
        'KNB' as data_source,
        current_timestamp() as dbt_staged_at

    from source
)

select * from staged