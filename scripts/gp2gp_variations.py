from collections import Counter
from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc

from prmdata.domain.spine.conversation import group_into_conversations
from prmdata.domain.spine.message import construct_messages_from_splunk_items
from prmdata.domain.spine.parsed_conversation import (
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
    COMMON_POINT_TO_POINT,
    APPLICATION_ACK,
)
from prmdata.utils.date.range import DateTimeRange
from prmdata.utils.io.csv import read_gzip_csv_files

input_files = [
    "./Jan-2021.csv.gz",
    "./Feb-2021.csv.gz",
]

metric_month = datetime(2021, 1, 1, tzinfo=tzutc())
next_month = metric_month + relativedelta(months=1)
date_range = DateTimeRange(metric_month, next_month)

abbreviations = {
    EHR_REQUEST_STARTED: "RQS",
    EHR_REQUEST_COMPLETED: "RQC",
    COMMON_POINT_TO_POINT: "P2P",
    APPLICATION_ACK: "ACK"
}


def read_spine_csv_gz_files(file_paths):
    items = read_gzip_csv_files(file_paths)
    return construct_messages_from_splunk_items(items)


def conversation_is_started_in(conversation, date_range):
    first_message = conversation.messages[0]
    return first_message.interaction_id == EHR_REQUEST_STARTED and date_range.contains(first_message.time)


def filter_for_full_conversations(conversations, date_range):
    return (c for c in conversations if conversation_is_started_in(c, date_range))


def message_code(message, requester):
    party = "R" if message.from_party_asid == requester else "S"
    interaction = abbreviations[message.interaction_id]
    error = str(message.error_code) if message.error_code is not None else ""
    return party + ":" + interaction + "[" + error + "]"


def extract_pattern(conversation):
    requester = conversation.messages[0].from_party_asid
    return "-".join((message_code(m, requester) for m in conversation.messages))


spine_messages = read_spine_csv_gz_files(input_files)
gp2gp_conversations = group_into_conversations(spine_messages)
full_conversations = filter_for_full_conversations(gp2gp_conversations, date_range)
counts = Counter((extract_pattern(c) for c in full_conversations))

with open("./gp2gp-patterns-jan-21.csv", 'w') as f:
    for pattern, count in counts.most_common():
        print(pattern, count)
