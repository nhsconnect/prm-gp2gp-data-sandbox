SELECT col2 AS ods_code,
         count(*) AS header_count
FROM raw_mi
WHERE col1 = 'HR'
        AND REGEXP_LIKE(col4, '^TPP')
        AND REGEXP_LIKE("$path",'Stats6_.{6}.dat$')
GROUP BY  col2