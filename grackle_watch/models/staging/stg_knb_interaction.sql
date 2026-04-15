-- stg_knb_interaction.sql
with source as (
    select * from {{ source('grackle_watch_external', 'knb_interaction') }}
),

staged as (
    select
        -- identifiers
        bird,
        interaction,
        trial,

        -- experimental conditions
        option_on_left,

        -- outcomes
        approached_first,
        put_more_food_on,

        -- temporal
        date as experiment_date,

        -- audit & provenance
        'KNB' as data_source,
        current_timestamp() as dbt_staged_at

    from source
)

select * from staged