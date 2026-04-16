-- fct_observations.sql
-- unified observation fact table combining GBIF and eBird sightings
-- primary fact table for all downstream Gold aggregations

with gbif as (
    select
        sighting_id,
        scientific_name,
        latitude,
        longitude,
        geo_location,
        state,
        event_date,
        year,
        month,
        cast(individual_count as integer) as individual_count,
        sex,
        data_source,
        dbt_staged_at
    from {{ ref('stg_gbif') }}
),

ebird as (
    select
        sighting_id,
        scientific_name,
        latitude,
        longitude,
        geo_location,
        state,
        event_date,
        year,
        month,
        cast(individual_count as integer) as individual_count,
        cast(null as string) as sex,
        data_source,
        dbt_staged_at
    from {{ ref('stg_ebird') }}
),

combined as (
    select * from gbif
    union all
    select * from ebird
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['data_source', 'sighting_id']) }} as observation_id,
        *
    from combined
)

select * from final