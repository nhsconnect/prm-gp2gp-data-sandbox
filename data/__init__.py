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
