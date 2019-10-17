import os
import glob
from collections import namedtuple

_INIT_FILE_PATH = os.path.realpath(__file__)
_DATA_DIR_PATH = os.path.dirname(_INIT_FILE_PATH)

DataSource = namedtuple("DataSource", ["description", "path", "columns"])

PRMT_365_Requestor_transfers = DataSource(
  description="",
  path=os.path.join(_DATA_DIR_PATH, "PRMT_365_Requestor_transfers.csv"),
  columns=None
)

PRMT_365_Sender_transfers = DataSource(
  description="",
  path=os.path.join(_DATA_DIR_PATH, "PRMT_365_Sender_transfers.csv"),
  columns=None
)

PRMT_365_Aggregated_signatures = DataSource(
  description="""
index="gp2gp-mi" sourcetype="gppractice-RR"
| where isnotnull(ConversationID) and RequestorODS != SenderODS
| eval key=RegistrationTime + "-" + RegistrationSmartcardUID
| eval Outcome=coalesce(ExtractAckStatus, "x") + "-" + coalesce(ExtractAckCode, "xx")
| eval Month=substr(RegistrationTime, 6, 2)
| stats values(Outcome) as Outcomes,
        dc(eval(Outcome="1-00" or Outcome="1-0")) as Integrated,
        dc(eval(Outcome="5-15")) as Suppressed
        by key, Month, SenderODS
| eval Signature=mvjoin(Outcomes, ",")
| stats count, sum(Integrated) as IntegratedCount, sum(Suppressed) as SuppressedCount by Signature, Month
  """,
  path=os.path.join(_DATA_DIR_PATH, "PRMT_365_Aggregated_signature.csv"),
  columns=None
)

PRMT_365_ODS_Supplier_Mapping = DataSource(
  description="""Mapping from GP ODS Code to supplier for 2019-05-01 to 2019-08-31. Only includes data for ODS codes where only
  one supplier was mentioned during that period.

  index="gp2gp-mi" souretype="gppractice-HR"
| rex field=RequestorSoftware "(?<Supplier>.*)_(?<System>.*)_(?<Version>.*)"
| stats min(ReportTimePeriod) as From,
        max(ReportTimePeriod) as To
        BY RequestorODS, Supplier
| stats values(eval(From + "--" + To + ":" + Supplier)) as SupplierDateRanges,
        values(Supplier) as Supplier,
        count
        BY RequestorODS
| where count=1
| eval ODSCode=RequestorODS
| table ODSCode, Supplier
  """,
  path=os.path.join(_DATA_DIR_PATH, "PRMT_365_ODS_to_Supplier_Mapping.csv"),
  columns=None
)

PRMT_372_attachment_sizes = DataSource(
  description="""
index="gp2gp-mi" sourcetype="gppractice-RR"
| eval key=RegistrationTime + "-" + RegistrationSmartcardUID
| eval month=substr(RegistrationTime, 6, 2)
| rex "Attachment size (after decompression)?: (?<attachment_size>\d+) is larger than TPP limit"
| eval attachment_size_mb=attachment_size / (1024 * 1024)
| search attachment_size=*
| table key, month, RequestorODS, attachment_size_mb​
  """,
  path=os.path.join(_DATA_DIR_PATH, "PRMT_372_attachment_sizes.csv"),
  columns=None
)

GP_ODS_Data = DataSource(
  description="""Retrieved from https://digital.nhs.uk/services/organisation-data-service/data-downloads/gp-and-gp-practice-related-data on 20191003.
  
  This CSV links ODS codes to GP practice addresses etc.
  """,
  path=os.path.join(_DATA_DIR_PATH, "epraccur.csv"),
  columns=["ODSCode", "Name", "NationalGrouping", "HighLevelHealthGeography",
      "AddressLine1","AddressLine2","AddressLine3","AddressLine4","AddressLine5","Postcode",
      "OpenDate","CloseDate","StatusCode","OrganisationSubTypeCode","Commissioner","JoinProviderOrPurchaserDate","LeftProviderOrPurchaserDate",
      "ContactTelephoneNumber",
      "Null1","Null2","Null3",
      "AmendedRecordIndicator",
      "Null4",
      "ProviderOrPurchaser",
      "Null5",
      "PrescribingSetting",
      "Null6"]
)

GP_CCG_Mapping = DataSource(
  description="""Retrieved from https://digital.nhs.uk/services/organisation-data-service/data-downloads/gp-and-gp-practice-related-data on 20191007.

  The CSV links GP Practice ODS codes through to parent organisations. Note that practices can move between parent organisation. The CSV is generated
  on a quarterly basis; this was generated on 20190830.
  """,
  path=os.path.join(_DATA_DIR_PATH, "epcmem.csv"),
  columns=["ODSCode", "ParentODSCode", "ParentOrganisationType", "JoinParentDate", "LeftParentDate", "AmendedRecordIndicator"]
)
