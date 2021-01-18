from gp2gp.service.models import ERROR_SUPPRESSED, Transfer, TransferStatus
from gp2gp.service.transformers import EIGHT_DAYS_IN_SECONDS, derive_transfers
from gp2gp.pipeline.dashboard.main import read_spine_csv_gz_files
from gp2gp.spine.transformers import parse_conversation, group_into_conversations, ConversationMissingStart

from collections import defaultdict

def parse_conversations(messages, time_range):
    for conversation in group_into_conversations(messages):
        try:
            gp2gp_conversation = parse_conversation(conversation)
            if time_range.contains(gp2gp_conversation.request_started.time):
                yield gp2gp_conversation
        except ConversationMissingStart:
            pass

def outcome(transfer: Transfer):
    if transfer.status == TransferStatus.FAILED:
        if transfer.final_error_code is not None:
            return "COMPLETED - ERROR IN FINAL ACK"
        elif len(transfer.intermediate_error_codes) > 0:
            return "DIDN'T COMPLETE - ERROR MID CONVERSATION"
        else: 
            raise Exception(f"Transfer with failed status but no errors. ConversationID: {transfer.conversation_id}")
            
          
    elif transfer.status == TransferStatus.INTEGRATED: 
        if transfer.sla_duration is None:
            raise Exception(f"Completed transfer had no SLA! {transfer.conversation_id}")
        elif transfer.sla_duration.total_seconds() <= EIGHT_DAYS_IN_SECONDS:
            return "COMPLETED - WITHIN 8 DAYS"
        else:
            return "COMPLETED - BEYOND 8 DAYS"
    elif transfer.status == TransferStatus.PENDING: 
        return "DIDN'T COMPLETE - STUCK"
    else: 
        raise Exception(f"Transfer with unknown status: {transfer.status}")


def calculate_counts(month_file_name: str, next_month_file_name: str, time_range):
  spine_messages = read_spine_csv_gz_files([
      month_file_name, next_month_file_name
  ])

  conversations = parse_conversations(spine_messages, time_range=time_range)
  transfers = derive_transfers(conversations)

  counts = defaultdict(int)
  for gp2g_transfer in transfers:
      counts[outcome(gp2g_transfer)] += 1

  return counts

