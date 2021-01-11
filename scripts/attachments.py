from pathlib import Path
from sys import argv
import duckdb

CREATE_ATTACHMENT_METADATA_TABLE_STATEMENT = """
    CREATE TABLE attachment_metadata (
            time TIMESTAMP,
            attachment_id VARCHAR,
            conversation_id VARCHAR,
            from_system VARCHAR,
            to_system VARCHAR,
            attachment_type VARCHAR,
            compressed BOOLEAN,
            content_type VARCHAR,
            large_attachment BOOLEAN,
            length INT,
            original_base64 BOOLEAN 
    );
"""

CREATE_YES_NO_MACRO_STATEMENT = """
    CREATE MACRO yes_no_to_bool(field) AS
    CASE WHEN field='Yes' THEN TRUE WHEN field='No' THEN FALSE ELSE NULL END
"""

CREATE_OPTIONAL_INT_MACRO_STATEMENT = """
    CREATE MACRO optional_int(field) AS
    CASE WHEN field='Unknown' THEN NULL ELSE CAST(field AS INT) END
"""


# TODO: Rewrite using relation api?
def load_attachment_metadata_statement(filename):
    return f"""
        INSERT INTO attachment_metadata
        SELECT
            strptime(left(_time, 23), '%Y-%m-%dT%H:%M:%S.%g') as time,
            attachmentId as attachment_id,
            conversationID as conversation_id,
            FromSystem as from_system,
            ToSystem as to_system,
            attachmentType as attachment_type,
            yes_no_to_bool(compressed) as compressed,
            contentType as content_type,
            yes_no_to_bool(largeAttachment) as large_attachment,
            optional_int(length) as length,
            yes_no_to_bool(originalBase64) as original_base64 
        FROM read_csv_auto('{filename}', all_varchar=TRUE);
    """


def load_attachment_metadata(cursor, input_data_dir):
    input_data_dir_path = Path(input_data_dir)
    attachments_data_files = input_data_dir_path.rglob("*csv")

    cursor.execute(CREATE_YES_NO_MACRO_STATEMENT)
    cursor.execute(CREATE_OPTIONAL_INT_MACRO_STATEMENT)
    cursor.execute(CREATE_ATTACHMENT_METADATA_TABLE_STATEMENT)

    for data_file in attachments_data_files:
        cursor.execute(load_attachment_metadata_statement(data_file))

    return cursor.table("attachment_metadata")


def create_view_attachments_count_per_conversation(attachment_metadata):
    return attachment_metadata \
        .aggregate("conversation_id, count(*) AS number_attachments") \
        .order("number_attachments desc") \
        .create_view("attachments_per_conversation")


def create_view_attachment_count_frequency(attachments_per_conversation):
    return attachments_per_conversation \
        .aggregate("number_attachments, count(*) AS transfer_count") \
        .order("number_attachments desc") \
        .create_view("number_attachment_frequency")


def create_view_attachment_statistics(attachment_metadata):
    return attachment_metadata \
        .aggregate("count(*) AS total,"
                   "MIN(length) AS smallest,"
                   "QUANTILE(length, 0.25) AS quantile_25,"
                   "MEDIAN(length) AS median,"
                   "QUANTILE(length, 0.75) AS quantile_75,"
                   "MAX(length) AS largest") \
        .create_view("attachment_statistics")


def construct_attachments_db(cursor, input_data_dir):
    attachment_metadata = load_attachment_metadata(cursor, input_data_dir)
    create_view_attachment_statistics(attachment_metadata)
    attachment_count_per_conversation = create_view_attachments_count_per_conversation(attachment_metadata)
    create_view_attachment_count_frequency(attachment_count_per_conversation)


def main():
    input_data_dir = argv[1]
    database_file = argv[2]
    cursor = duckdb.connect(database_file)
    construct_attachments_db(cursor, input_data_dir)
    cursor.close()


if __name__ == "__main__":
    main()
