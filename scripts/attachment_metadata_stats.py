import duckdb
from sys import argv


# These can be run after running the attachment_insights script
def create_view_attachments_count_per_conversation(attachment_metadata):
    return attachment_metadata\
        .aggregate("conversation_id, count(*) AS number_attachments")\
        .order("number_attachments desc")\
        .create_view("attachments_per_conversation")


def create_view_attachment_count_frequency(attachments_per_conversation):
    return attachments_per_conversation \
        .aggregate("number_attachments, count(*) AS count") \
        .order("count desc") \
        .create_view("number_attachment_frequency")


def main():
    attachment_metadata_db_path = argv[1]
    cursor = duckdb.connect(attachment_metadata_db_path)
    attachment_metadata = cursor.table("attachment_metadata")

    attachment_count_per_conversation = create_view_attachments_count_per_conversation(attachment_metadata)
    print(f"Most number of attachments per conversation: {attachment_count_per_conversation.limit(3)}")

    attachment_count_frequency = create_view_attachment_count_frequency(attachment_count_per_conversation)
    print(f"Most frequent number of attachments associated in a transfer: {attachment_count_frequency.limit(3)}")


if __name__ == "__main__":
    main()