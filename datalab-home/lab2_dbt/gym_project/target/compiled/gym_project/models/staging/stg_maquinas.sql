

with source as (
    select * from "datalab"."public"."maquinas_gym"
),

renamed as (
    select
        id_maquina,
        upper(marca) as marca,
        upper(modelo) as modelo,
        resistencia_max_lbs,
        ultima_mantenimiento,
        upper(estado) as estado
    from source
)

select * from renamed