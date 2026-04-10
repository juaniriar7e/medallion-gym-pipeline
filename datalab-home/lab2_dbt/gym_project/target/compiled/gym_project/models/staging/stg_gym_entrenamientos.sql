

with raw_data as (
    -- Cambiamos 'entrenamientos_sensores' por 'entrenamientos_raw'
    select * from "datalab"."public"."entrenamientos_raw"
),

final as (
    select
        -- Generamos la llave usando las columnas reales de tu script
        md5(cast(coalesce(cast(user_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(timestamp as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(machine_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as entrenamiento_id,
        
        user_id::int as usuario_id,
        machine_id::int as maquina_id,
        machine_name::varchar(100) as nombre_maquina,
        
        -- En el script de Python, timestamp ya es fecha, pero el cast no sobra
        timestamp::timestamp as fecha_entrenamiento,
        
        weight_kg::decimal(10,2) as peso_kg,
        reps::int as repeticiones,
        rpe::int as rpe_esfuerzo
        
    from raw_data
    where user_id is not null
)

select * from final