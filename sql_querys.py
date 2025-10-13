# Genéricos
truncate_sql_cascade = """
    TRUNCATE TABLE {{ params.table_name }} CASCADE;
"""

truncate_sql = """
    TRUNCATE TABLE {{ params.table_name }};
"""

# SIS parte 11

insert_beneficios_sql = """
        INSERT INTO beneficios_af (solicitud_id, tipo_de_pedido, pedido, tipo_resultado, resultado)
        SELECT s._id AS solicitud_id, 
               tipo_de_pedido.nombre AS tipo_de_pedido, 
               pedido.detalle AS pedido,
               MAX(tipo_de_resultado.nombre) AS tipo_resultado,
               MAX(
                   CASE
                       WHEN tipo_de_resultado.id IS NOT NULL THEN resultado.detalle
                       ELSE NULL
                   END
               ) AS resultado
        FROM solicitudes s
        INNER JOIN avances av_pedido ON s._id = av_pedido.solicitud_id
        INNER JOIN metadatos pedido ON pedido.avance_id = av_pedido._id
        INNER JOIN tipometadato tipo_de_pedido ON pedido.tipometadato_id = tipo_de_pedido._id 
            AND tipo_de_pedido.nombre = 'Tipo de beneficio'
        LEFT JOIN avances av_resultado ON s._id = av_resultado.solicitud_id
        LEFT JOIN metadatos resultado ON resultado.avance_id = av_resultado._id
        LEFT JOIN tipometadato tipo_de_resultado ON resultado.tipometadato_id = tipo_de_resultado._id 
            AND tipo_de_resultado.nombre = CONCAT('Resultado: ', COALESCE(pedido.detalle, 'N/A'))
        WHERE (s.parent_area = 'Dirección Instituto de Clasificación')
           OR (s.parent_area = 'Dirección de asistencia y tratamiento - SPB')
        GROUP BY s._id, tipo_de_pedido.nombre, pedido.detalle;
        """

update_beneficio_acta_sql = """
        UPDATE beneficios_af baf
        SET fecha_acta = da.fecha::date
        FROM solicitudes s
        INNER JOIN avances av ON s._id = av.solicitud_id
        INNER JOIN metadatos da ON da.avance_id = av._id
        INNER JOIN tipometadato tipo_da ON da.tipometadato_id = tipo_da._id AND tipo_da.nombre = 'Fecha del acta'
        WHERE baf.solicitud_id = s._id;
        """

update_beneficio_fecha_sql = """
        UPDATE beneficios_af baf
        SET fecha_spb = CAST(da.fecha AS DATE)
        FROM solicitudes s
        INNER JOIN avances av ON s._id = av.solicitud_id
        INNER JOIN metadatos da ON da.avance_id = av._id
        INNER JOIN tipometadato tipo_da ON da.tipometadato_id = tipo_da._id AND tipo_da.nombre = 'Fecha de salida SPB'
        WHERE baf.solicitud_id = s._id;
        """

update_beneficio_numero_sql = """
        UPDATE beneficios_af
        SET numero_acta = da.detalle
        FROM solicitudes s
        INNER JOIN avances av ON s._id = av.solicitud_id
        INNER JOIN metadatos da ON da.avance_id = av._id
        INNER JOIN tipometadato tipo_da ON da.tipometadato_id = tipo_da._id AND tipo_da.nombre = 'Número del acta'
        WHERE beneficios_af.solicitud_id = s._id;
        """

# SIS parte 10

insert_initial_intervenciones_poblacion_sql = """
     INSERT INTO {{ params.table_name }} (id_solicitud, id_usuario,fecha,tipo_intervencion)
    SELECT _id,usuario_id,fechaIngreso,'Carga' as tipo_intervencion
    FROM {{ params.table_solicitudes }} S
    WHERE S.area = 'Dirección de Población en Contexto de Encierro y Derechos Humanos'
        AND S.fechaIngreso >= '2023-05-02';
"""

insert_metadatos_intervenciones_poblacion_sql = """
    INSERT INTO {{ params.table_name }} (id_solicitud, id_usuario, fecha, tipo_intervencion)
SELECT 
    av.solicitud_id,
    av.usuario_id,
    av.fecha,
    tm.nombre
FROM 
    {{ params.table_avances }} av
INNER JOIN 
    {{ params.table_solicitudes }} s ON av.solicitud_id = s._id
INNER JOIN 
    {{ params.table_metadatos }} m ON av._id = m.avance_id
INNER JOIN 
    {{ params.table_tipometadato }} tm ON m.tipometadato_id = tm._id
WHERE 
    (tm.nombre = 'Reiteratorio' OR tm.nombre = 'Informe')
    AND s.area = 'Dirección de Población en Contexto de Encierro y Derechos Humanos'
    AND s.fechaIngreso >= '2023-05-02';

"""

update_usernames_intervenciones_poblacion_sql = """
    UPDATE Intervenciones_poblacion
    SET nombre_usuario = u.nombre
    FROM usuarios AS u
    WHERE Intervenciones_poblacion.id_usuario = u._id;

"""

update_tipo_subtipo_intervenciones_poblacion_sql = """
  UPDATE public.intervenciones_poblacion ip
  SET 
      tipo = CASE
          WHEN ip.tipo_intervencion = 'Informe' THEN 'Aval de traslado'
          ELSE s.tipo
      END,
      subtipo = CASE
          WHEN ip.tipo_intervencion = 'Informe' THEN ip.detalle
          ELSE s.subtipo
      END
  FROM public.solicitudes s
  WHERE ip.id_solicitud = s._id;
"""

# SIS parte 9

actualizar_creador_en_solicitudes_query = """

    WITH primer_avance AS (
        SELECT DISTINCT ON (a.solicitud_id)
            a.solicitud_id,
            a.usuario_id
        FROM avances a
        ORDER BY a.solicitud_id, a.fecha ASC
    )
    UPDATE solicitudes s
    SET usuario_id = pa.usuario_id
    FROM primer_avance pa
    WHERE s._id = pa.solicitud_id;
"""
  

actualizar_tipo_lugar_en_solicitudes_query = """
    UPDATE {{ params.table_solicitudes }} s
SET {{ params.campo_nombre_tipo_lugar }} = tl.nombre
FROM {{ params.table_tipolugar }} tl
WHERE s.{{ params.campo_solicitud_id }} = tl._id;

"""

actualizar_area_parent_en_solicitudes_query = """
    UPDATE {{ params.table_solicitudes }} s
SET 
    {{ params.campo_nombre_area }} = a.nombre,
    parent_area = pa.nombre
FROM {{ params.table_areas }} a
LEFT JOIN {{ params.table_areas }} pa ON a.parent = pa._id
WHERE s.{{ params.campo_solicitud_id }} = a._id;

"""

actualizar_canal_en_solicitudes_query = """
    UPDATE {{ params.table_solicitudes }} s
SET {{ params.campo_nombre_canal }} = c.nombre
FROM {{ params.table_canal }} c
WHERE s.{{ params.campo_solicitud_id }} = c._id;

"""

actualizar_tipo_subtipo_en_solicitudes_query = """
UPDATE {{ params.table_solicitudes }} s
SET 
    {{ params.campo_tipo_nombre }} = t.nombre, 
    {{ params.campo_subtipo_nombre }} = st.nombre
FROM {{ params.table_tipopeticions }} st
LEFT JOIN {{ params.table_tipopeticions }} t ON st.tipo = t._id
WHERE s.{{ params.campo_subtipo }} = st._id;

"""

actualizar_estado_en_solicitudes_query = """
    UPDATE {{ params.table_solicitudes }} s
SET {{ params.campo_estado_nombre }} = e.nombre
FROM {{ params.table_avanceestados }} e
WHERE s.{{ params.campo_estado_id }} = e._id 
  AND e.nombre IS NOT NULL;

"""

actualizar_usuario_areaCarga_en_solicitudes_query = """
   UPDATE {{ params.table_solicitudes }} s
SET 
    {{ params.campo_solicitudes_usuario_carga }} = CONCAT(u.nombre, ' ', u.apellido), 
    {{ params.campo_solicitudes_area_carga }} = ac.nombre
FROM {{ params.table_usuarios }} u
LEFT JOIN {{ params.table_areas }} ac ON u.{{ params.campo_usuarios_area_id }} = ac._id
WHERE s.{{ params.campo_solicitudes_usuario_id }} = u.{{ params.campo_usuarios_id }};

"""

actualizar_complejo_en_solicitudes_query = """
    UPDATE {{ params.table_solicitudes }} s
SET {{ params.campo_solicitudes_complejo }} = u.{{ params.campo_unidades_complejo }}
FROM {{ params.table_unidades }} u
WHERE {{ params.campo_solicitudes_lugar }} = u.{{ params.campo_unidades_nombre }};

"""

actualizar_entidades_juridicas_query = """
   UPDATE {{ params.table_entidades }}
SET {{ params.campo_entidades_departamento_judicial }} = TRIM(SPLIT_PART({{ params.campo_entidades_nombre }}, '-', 2));

"""

actualizar_datos_sin_joins_en_solicitudes_query = """
    UPDATE {{ params.table_solicitudes }}
    SET 
        tipolugar = tl.nombre,
        area = a.nombre,
        canal = c.nombre,
        parent_area = pa.nombre,
        tipo = t.nombre,
        subtipo = st.nombre,
        usuario_carga = CONCAT(u.nombre, ' ', u.apellido),
        area_carga = ac.nombre
    FROM {{ params.table_solicitudes }} s
        LEFT JOIN {{ params.table_tipolugar }} tl ON s.tipolugar_id = tl._id
        LEFT JOIN {{ params.table_areas }} a ON s.area_id = a._id
        LEFT JOIN {{ params.table_areas }} pa ON a.parent = pa._id
        LEFT JOIN {{ params.table_canal }} c ON s.canal_id = c._id
        LEFT JOIN {{ params.table_tipopeticions }} st ON s.subtipo_id = st._id
        LEFT JOIN {{ params.table_tipopeticions }} t ON st.tipo = t._id
        LEFT JOIN {{ params.table_usuarios }} u ON s.usuario_id = u._id
        LEFT JOIN {{ params.table_areas }} ac ON u.area = ac._id
    WHERE {{ params.table_solicitudes }}._id = s._id;
"""

# SIS parte 8

insert_sql = """
    INSERT INTO {{ params.DA_AfectadaEdadGenero_table }} (
        id_solicitud,
        avance_estado,
        fecha_avance,
        tipo_metadato,
        metadato,
        edad,
        afectada_genero,
        id_afectada,
        afectada_nacionalidad,
        afectada_ocupacion,
        afectada_rango_etario,
        afectada_rango_etario_infantil
    )
    SELECT
        s._id AS id_solicitud,
        ave.nombre AS avance_estado,
        av.fecha AS fecha_avance,
        tm.nombre AS tipo_metadato,
        m.detalle AS metadato,
        af.edad,
        af.genero AS afectada_genero,
        af._id AS id_afectada,
        af.nacionalidad AS afectada_nacionalidad,
        af.ocupacion AS afectada_ocupacion,
        af.rango_etario,
        af.rango_etario_infantil
    FROM
        {{ params.solicitudes_table }} s
        INNER JOIN {{ params.afectada_table }} af ON s._id = af.solicitud_id
        LEFT JOIN {{ params.avances_table }} av ON s._id = av.solicitud_id
        LEFT JOIN {{ params.avanceestados_table }} ave ON av.estado = ave._id
        LEFT JOIN {{ params.metadatos_table }} m ON av._id = m.avance_id
        LEFT JOIN {{ params.tipometadato_table }} tm ON m.tipometadato_id = tm._id;
"""

insert_sql_da_afectadaedadgenero = """

    SET statement_timeout = '15min';

    INSERT INTO {{ params.DA_AfectadaEdadGenero_table }}  (
        id_solicitud,
        avance_estado,
        fecha_avance,
        tipo_metadato,
        metadato,
        edad,
        afectada_genero,
        id_afectada,
        afectada_nacionalidad,
        afectada_ocupacion,
        afectada_rango_etario,
        afectada_rango_etario_infantil,
        fecha,
        hora,
        fechaIngreso,
        ultimo_estado,
        fechaUltimoAvance,
        lugar,
        tipolugar,
        area,
        canal,
        parent_area,
        tipo,
        subtipo,
        urgencia
    )
    SELECT
        s._id AS id_solicitud,
        ave.nombre AS avance_estado,
        av.fecha AS fecha_avance,
        tm.nombre AS tipo_metadato,
        m.detalle AS metadato,
        af.edad,
        af.genero AS afectada_genero,
        af._id AS id_afectada,
        af.nacionalidad AS afectada_nacionalidad,
        af.ocupacion AS afectada_ocupacion,
        af.rango_etario,
        af.rango_etario_infantil,
        s.fecha,
        s.hora,
        s.fechaIngreso,
        s.estado AS ultimo_estado,
        s.fechaUltimoAvance,
        s.lugar,
        tl.nombre AS tipolugar,
        a.nombre AS area,
        c.nombre AS canal,
        pa.nombre AS parent_area,
        t.nombre AS tipo,
        st.nombre AS subtipo,
        s.urgencia
    FROM
        {{ params.solicitudes_table }} s
        INNER JOIN {{ params.afectada_table }} af ON s._id = af.solicitud_id
        LEFT JOIN {{ params.avances_table }} av ON s._id = av.solicitud_id
        LEFT JOIN {{ params.avanceestados_table }} ave ON av.estado = ave._id
        LEFT JOIN {{ params.metadatos_table }} m ON av._id = m.avance_id
        LEFT JOIN {{ params.tipometadato_table }} tm ON m.tipometadato_id = tm._id
        LEFT JOIN {{ params.tipolugar_table }} tl ON s.tipolugar_id = tl._id
        LEFT JOIN {{ params.areas_table }} a ON s.area_id = a._id
        LEFT JOIN {{ params.areas_table }} pa ON a.parent = pa._id
        LEFT JOIN {{ params.canal_table }} c ON s.canal_id = c._id
        LEFT JOIN {{ params.tipopeticions_table }} st ON s.subtipo_id = st._id
        LEFT JOIN {{ params.tipopeticions_table }} t ON st.tipo = t._id
"""

update_sql = """
  UPDATE {{ params.DA_AfectadaEdadGenero_table }} DA
SET
    fecha = s.fecha,
    hora = s.hora,
    fechaIngreso = s.fechaIngreso,
    ultimo_estado = s.estado,
    fechaUltimoAvance = s.fechaUltimoAvance,
    lugar = s.lugar,
    tipolugar = tl.nombre,
    area = a.nombre,
    canal = c.nombre,
    parent_area = pa.nombre,
    tipo = t.nombre,
    subtipo = st.nombre,
    urgencia = s.urgencia
FROM {{ params.solicitudes_table }} s
LEFT JOIN {{ params.tipolugar_table }} tl ON s.tipolugar_id = tl._id
LEFT JOIN {{ params.areas_table }} a ON s.area_id = a._id
LEFT JOIN {{ params.areas_table }} pa ON a.parent = pa._id
LEFT JOIN {{ params.canal_table }} c ON s.canal_id = c._id
LEFT JOIN {{ params.tipopeticions_table }} st ON s.subtipo_id = st._id
LEFT JOIN {{ params.tipopeticions_table }} t ON st.tipo = t._id
WHERE DA.id_solicitud = s._id;

"""

# SIS parte 7

sql_update_single_opt = """
    UPDATE {{ params.table_name }} a
    SET {{ params.campo }} = s._id
    FROM (
        SELECT _id, {{ params.campo_single }}
        FROM {{ params.table_solicitudes }}
        WHERE {{ params.campo_single }} IS NOT NULL
        LIMIT 1000
    ) s
    WHERE s.{{ params.campo_single }} = a._id;

"""

# Otros

sql_update_single = """
   UPDATE {{ params.table_name }} a
    SET {{ params.campo }} = s._id
    FROM {{ params.table_solicitudes }} s
    WHERE s.{{ params.campo_single }} = a._id
    AND s.{{ params.campo_single }} IS NOT NULL;

"""

sql_update_single_solicitud = """
    UPDATE {{ params.table_name }} s
    SET {{ params.campo_single }} = {{ params.campo }}
    WHERE {{ params.campo }} IS NOT NULL 
    AND {{ params.campo }} <> '' 
    AND {{ params.campo }} NOT LIKE '%,%'


"""

sql_ranges_infantil = """
    UPDATE {{ params.table_name }}
SET rango_etario_infantil = CASE
    WHEN edad LIKE '%mes%' THEN 'Menor de 6'
    WHEN edad !~ '^[0-9]+$' THEN 'Sin completar'  -- Usar !~ para negar coincidencias de expresiones regulares
    WHEN edad::int BETWEEN 0 AND 6 THEN 'Menor de 6'
    WHEN edad::int BETWEEN 7 AND 12 THEN 'De 7 a 12'
    WHEN edad::int BETWEEN 13 AND 18 THEN 'De 13 a 18'
    WHEN edad::int BETWEEN 19 AND 25 THEN 'De 19 a 25'
    WHEN edad::int BETWEEN 26 AND 50 THEN 'De 26 a 50'
    WHEN edad::int >= 51 THEN 'Mayores a 51'
    ELSE 'Sin completar'
END;

"""

sql_ranges = """
   UPDATE {{ params.table_name }}
SET rango_etario = CASE
    WHEN edad LIKE '%mes%' THEN 'Menor de 18'
    WHEN edad !~ '^[0-9]+$' THEN 'Sin completar'  -- Usar !~ para negar expresiones regulares
    WHEN edad::int < 18 THEN 'Menor de 18'
    WHEN edad::int BETWEEN 18 AND 25 THEN 'De 18 a 25'
    WHEN edad::int BETWEEN 26 AND 50 THEN 'De 26 a 50'
    WHEN edad::int >= 51 THEN 'Mayores a 51'
    ELSE 'Sin completar'
END;

"""
sql_update_fields = """
UPDATE {{ params.table_name }} AS a
    SET tipoidentificacion = (SELECT ti.nombre 
                          FROM {{ params.tipoidentificacion_table_name }} AS ti
                          WHERE a.tipoIdentificacionId = ti._id),
    genero = (SELECT g.nombre
              FROM {{ params.generos_table_name }} AS g
              WHERE a.generoId = g._id)
WHERE a.tipoIdentificacionId IN (SELECT ti._id FROM {{ params.tipoidentificacion_table_name }} AS ti);
"""

sql_update_fields_2 = """
   UPDATE {{ params.table_name }} AS a
    SET tipoidentificacion = ti.nombre,
        genero = g.nombre
    FROM {{ params.tipoidentificacion_table_name }} AS ti
    LEFT JOIN {{ params.generos_table_name }} AS g ON a.generoId = g._id
    WHERE a.tipoIdentificacionId = ti._id;

"""

sql_get_mysql = """
    SELECT _id, {{ params.field }}, '{{ params.table }}' AS tabla
    FROM {{ params.table_origin }}
    WHERE (
        {{ params.field }} IS NOT NULL AND
        {{ params.field }} <> '' AND
        (LENGTH({{ params.field }}) - LENGTH(REPLACE({{ params.field }}, ',', ''))) > 0
    );

"""
