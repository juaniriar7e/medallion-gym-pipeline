{{ config(
    materialized='table',
    schema='gold'
) }}

with silver_data as (
    select * from {{ ref('stg_gym_entrenamientos') }}
),

metrics as (
    select
        usuario_id,
        nombre_maquina,
        fecha_entrenamiento::date as fecha,
        repeticiones,
        peso_kg,
        rpe_esfuerzo,
        -- Cálculo de Volumen Total
        (peso_kg * repeticiones) as volumen_total,
        -- Estimación de 1RM usando la fórmula de Brzycki: Peso / (1.0278 - (0.0278 * Reps))
        case 
            when repeticiones > 0 then round(peso_kg / (1.0278 - (0.0278 * repeticiones)), 2)
            else 0 
        end as one_rep_max_est
    from silver_data
)

select
    usuario_id,
    nombre_maquina,
    fecha,
    sum(volumen_total) as volumen_diario,
    max(one_rep_max_est) as mejor_1rm_estimado,
    avg(rpe_esfuerzo) as rpe_promedio_diario,
    count(*) as total_series
from metrics
group by 1, 2, 3
order by fecha desc, mejor_1rm_estimado desc
