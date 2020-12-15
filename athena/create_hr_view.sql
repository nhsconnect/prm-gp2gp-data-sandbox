CREATE OR REPLACE VIEW mi_hr AS 
SELECT
  col1 as "RecordType",
  col2 as "RequesterODS",
  col3 as "RequestorASID",
  col4 as "RequestorSoftware",
  col5 as "RequestorApplicationStatus",
  col6 as "ReportTimePeriod",
  col7 as "MaxMessageSize",
  col8 as "MaxNumAttachments",
  col9 as "MaxAttachSize",
  col10 as "ConfigA",
  col11 as "ConfigB"
FROM
  raw_mi
WHERE col1='HR'