from gp2gp.service.models import ERROR_SUPPRESSED
from gp2gp.service.transformers import EIGHT_DAYS_IN_SECONDS, derive_transfers
from gp2gp.date.range import DateTimeRange
from gp2gp.pipeline.dashboard.main import read_spine_csv_gz_files
from gp2gp.spine.transformers import parse_conversation, group_into_conversations, ConversationMissingStart

from collections import defaultdict
from datetime import datetime
from dateutil.tz import tzutc

august_data_file_name="spine_aug.csv.gz"
september_data_file_name = "spine_sep.csv.gz"

august = DateTimeRange(
    datetime(year=2020, month=8, day=1, tzinfo=tzutc()),
    datetime(year=2020, month=9, day=1, tzinfo=tzutc()),
)

def parse_conversations(messages, time_range):
    for conversation in group_into_conversations(messages):
        try:
            gp2gp_conversation = parse_conversation(conversation)
            if time_range.contains(gp2gp_conversation.request_started.time):
                yield gp2gp_conversation
        except ConversationMissingStart:
            pass

def outcome(transfer):
    if transfer.pending: # "is not completed"
        return "DIDN'T COMPLETE - POSSIBLE ERROR OR STILL WAITING"
    else:
        if transfer.error_code is None or transfer.error_code == ERROR_SUPPRESSED: # "final error code"
            if transfer.sla_duration is None:
                raise Exception(f"Completed transfer had no SLA! {transfer.conversation_id}")
            elif transfer.sla_duration.total_seconds() <= EIGHT_DAYS_IN_SECONDS:
                return "COMPLETED - WITHIN 8 DAYS"
            else:
                return "COMPLETED - BEYOND 8 DAYS"
        else:
            return "COMPLETED - ERROR"

spine_messages = read_spine_csv_gz_files([
    august_data_file_name, september_data_file_name
])

conversations = parse_conversations(spine_messages, time_range=august)
transfers = derive_transfers(conversations)

counts = defaultdict(int)
for gp2g_transfer in transfers:
    counts[outcome(gp2g_transfer)] += 1

print(counts)

