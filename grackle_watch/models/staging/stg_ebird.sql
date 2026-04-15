with source as (
    select * from {{ source('grackle_watch_external', 'ebird') }}
),

staged as (
    select
        -- identifiers
        checklist_id as sighting_id,  -- str, unique identifier for each eBird checklist 
        location_id,
        location_name,

        -- taxonomy
        scientific_name,
        common_name,
        species_code,

        -- spatial
        latitude,
        longitude,
        st_geogpoint(longitude, latitude) as geo_location,  -- native geography object for spatial queries
        state,

        -- temporal
        event_date,
        year,
        month,

        -- biological / behavioral
        cast(count as integer) as individual_count,  -- renamed to avoid SQL reserved word conflict
        is_valid,
        is_reviewed,
        location_private,

        -- audit & provenance
        'eBird' as data_source,               -- hardcoded source label for cross-dataset unions
        current_timestamp() as dbt_staged_at  -- pipeline run timestamp for monitoring

    from source
)

select * from staged