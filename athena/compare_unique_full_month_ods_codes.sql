SELECT COUNT(a.reporting_period_ods_code)
FROM 
    (SELECT DISTINCT reporting_period_ods_code
    FROM 
        (SELECT col2 AS reporting_period_ods_code,
         col6 AS reporting_period,
         "$path" AS s3key
        FROM raw_mi
        WHERE col1='HR'
                AND REGEXP_LIKE("$path",'Stats6_.{6}.dat$')
                AND REGEXP_LIKE(col4, '^TPP'))) AS a
    INNER JOIN 
    (SELECT ods_code AS tpp_ods_code
    FROM unique_tpp_ods_codes_feb) AS b
    ON a.reporting_period_ods_code=b.tpp_ods_code;