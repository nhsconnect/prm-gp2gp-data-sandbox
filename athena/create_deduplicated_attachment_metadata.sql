create or replace view gp2gp_attachment_ehr_message_ids as select
  distinct internal_id
  from gp2gp_attachment_messages
  where interaction_id='urn:nhs:names:services:gp2gp/RCMR_IN030000UK06';

create or replace view gp2gp_attachments as
select
  gp2gp_attachment_ehr_message_ids.internal_id,
  cast(from_iso8601_timestamp(time) as timestamp) as time,
  attachment_id,
  conversation_id,
  from_system,
  to_system,
  attachment_type,
  case when compressed='Yes' then TRUE when compressed='No' then FALSE else NULL end as compressed,
  content_type,
  case when large_attachment='Yes' then TRUE when large_attachment='No' then FALSE else NULL end as large_attachment,
  case when length='Unknown' then NULL else cast(length as integer) end as length,
  case when original_base64='Yes' then TRUE when original_base64='No' then FALSE else NULL end as original_base64
from gp2gp_attachment_ehr_message_ids
left join gp2gp_attachment_metadata
  on gp2gp_attachment_metadata.internal_id = gp2gp_attachment_ehr_message_ids.internal_id;


create table gp2gp_attachments_out
    with (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = 's3://prm-gp2gp-data-sandbox-dev/attachment-insights/gp2gp_attachments_q1_2021',
      bucket_count = 1,
      bucketed_by = ARRAY['internal_id']
    )
    as select * from gp2gp_attachments
