CREATE OR REPLACE VIEW mi_fr AS
SELECT col1 AS "RecordType",
         col2 AS "RequestRecordCount",
         col3 AS "SendRecordCount",
         col4 AS "RequestorODS",
         col5 AS "ReportTimePeriod"
FROM raw_mi
WHERE col1='FR'