create
        OR replace view gp2gp_ehr_attachment_summary AS
SELECT ids.internal_id,
         coalesce(attachment_count,
         0) AS attachment_count
FROM gp2gp_attachment_ehr_message_ids AS ids
LEFT JOIN
    (SELECT internal_id,
         count(*) AS attachment_count
    FROM gp2gp_attachment_metadata
    GROUP BY  internal_id ) counts
    ON ids.internal_id = counts.internal_id;

CREATE table gp2gp_ehr_attachment_summary_out
WITH ( format = 'PARQUET', parquet_compression = 'SNAPPY', external_location = 's3://prm-gp2gp-data-sandbox-dev/attachment-insights/gp2gp_ehr_attachment_summary_q1_2021', bucket_count = 1, bucketed_by = ARRAY['internal_id'] ) AS
SELECT *
FROM gp2gp_ehr_attachment_summary