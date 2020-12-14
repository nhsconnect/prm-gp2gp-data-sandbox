SELECT COUNT(DISTINCT ods_code)
FROM 
    (SELECT col2 AS ods_Code,
         col6 AS reporting_period,
         "$path" AS s3key
    FROM raw_mi
    WHERE col1='HR'
            AND REGEXP_LIKE("$path",'Stats([6-9]|10)_.{6}.dat$')
            AND REGEXP_LIKE(col4, '^TPP'));