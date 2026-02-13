--SQL SCRIPT TO CREATE OBJECT DEFINTIONS RESULT SET

SELECT  
DatabaseName = DB_NAME(),
    OBJECT_SCHEMA_NAME(m.object_id) AS [Schema],
    OBJECT_NAME(m.object_id) AS [Object],
    o.type_desc AS ObjectType,
    REPLACE(REPLACE(m.definition, CHAR(13) + CHAR(10), '\n'), CHAR(9), '\t') AS definition
FROM sys.sql_modules m
JOIN sys.objects o 
    ON m.object_id = o.object_id;
--

--SQL SCRIPT TO CREATE DEPENCENCY RESULT SET
SELECT
    DB_NAME() AS [Database],
    d.referencing_id                              AS [Dependent_Object_ID],
    OBJECT_SCHEMA_NAME(d.referencing_id)          AS [Dependent_Schema],
    OBJECT_NAME(d.referencing_id)                 AS [Dependent_Object_Name],
    o1.type_desc                                  AS [Dependent_Object_Type],

    -- Prefer name-based info to avoid losing unresolved refs; include resolved ID when available
    d.referenced_id                               AS [Depends_On_Object_ID],
    COALESCE(d.referenced_schema_name, OBJECT_SCHEMA_NAME(d.referenced_id)) AS [Depends_On_Schema],
    COALESCE(d.referenced_entity_name,  OBJECT_NAME(d.referenced_id))       AS [Depends_On_Object_Name],

    -- Type desc: resolved object type, or SYNONYM when matched in sys.synonyms, else NULL
    COALESCE(
        o2.type_desc,
        CASE 
            WHEN syn_by_id.object_id IS NOT NULL              THEN 'SYNONYM'
            WHEN syn_by_name.name IS NOT NULL                 THEN 'SYNONYM'
        END
    )                                              AS [Depends_On_Object_Type],

    CONCAT(
        '/',
        COALESCE(d.referenced_schema_name, OBJECT_SCHEMA_NAME(d.referenced_id)), '.', 
        COALESCE(d.referenced_entity_name,  OBJECT_NAME(d.referenced_id)),
        '/',
        OBJECT_SCHEMA_NAME(d.referencing_id), '.', OBJECT_NAME(d.referencing_id)
    )                                              AS [Object_Hierarchy]

FROM sys.sql_expression_dependencies AS d
INNER JOIN sys.objects AS o1
    ON d.referencing_id = o1.object_id

-- Keep referenced side as LEFT JOIN so unresolved references are still returned
LEFT JOIN sys.objects  AS o2
    ON d.referenced_id = o2.object_id

-- Surface synonyms whether referenced_id resolves or only names are present
LEFT JOIN sys.synonyms AS syn_by_id
    ON syn_by_id.object_id = d.referenced_id
LEFT JOIN sys.synonyms AS syn_by_name
    ON syn_by_name.name = d.referenced_entity_name
   AND SCHEMA_NAME(syn_by_name.schema_id) = d.referenced_schema_name
ORDER BY
    [Dependent_Object_Name],
    [Depends_On_Object_Name];