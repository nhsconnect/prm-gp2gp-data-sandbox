from gp2gp.spine.models import COMMON_POINT_TO_POINT, EHR_REQUEST_COMPLETED
from sys import argv
from gp2gp.io.csv import read_gzip_csv_files
from gp2gp.spine.sources import construct_messages_from_splunk_items
from gp2gp.spine.transformers import (
    group_into_conversations,
    parse_conversation,
)


def parse_conversations(conversations):
    return [parse_conversation(conversation)
            for conversation in conversations
            if parse_conversation(conversation) is not None]


def process_messages(messages):
    return group_into_conversations(messages)


def main():
    input_file_name = argv[1]
    csv_file_content = read_gzip_csv_files([input_file_name])
    spine_messages = construct_messages_from_splunk_items(csv_file_content)
    conversations = process_messages(spine_messages)

    transfers_exceeding_24h = []
    for conversation in conversations:
        sla = None
        start = None
        point_to_point_messages_time = []

        for message in conversation.messages:
            if message.interaction_id == EHR_REQUEST_COMPLETED:
                start = message.time

            if message.interaction_id == COMMON_POINT_TO_POINT:
                point_to_point_messages_time.append(message.time)

        if start is not None and len(point_to_point_messages_time) > 0:
            point_to_point_messages_time.sort()
            sla = point_to_point_messages_time[-1] - start

        if sla is not None and sla.total_seconds() > 86400:
            transfers_exceeding_24h.append(conversation.id)

    return transfers_exceeding_24h


if __name__ == "__main__":
    transfers = main()
    print("\n".join(transfers))
