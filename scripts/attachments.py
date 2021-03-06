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
            original_base64 BOOLEAN,
            internal_id VARCHAR
    );
"""

CREATE_GP2GP_MESSAGES_TABLE_STATEMENT = """
    CREATE TABLE gp2gp_messages (
            time TIMESTAMP,
            conversation_id VARCHAR,
            internal_id VARCHAR,
            interaction_id VARCHAR
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
            yes_no_to_bool(originalBase64) as original_base64,
            internalID as internal_id
        FROM read_csv_auto('{filename}', all_varchar=TRUE);
    """


def load_gp2gp_messages_statement(filename):
    return f"""
        INSERT INTO gp2gp_messages
        SELECT
            strptime(left(_time, 23), '%Y-%m-%dT%H:%M:%S.%g') as time,
            conversationID as conversation_id,
            internalID as internal_id,
            interactionID as interaction_id
        FROM read_csv_auto('{filename}', all_varchar=TRUE, header=TRUE);
    """


def construct_attachments_db(cursor, input_data_dir):
    input_data_dir_path = Path(input_data_dir)

    cursor.execute(CREATE_YES_NO_MACRO_STATEMENT)
    cursor.execute(CREATE_OPTIONAL_INT_MACRO_STATEMENT)
    cursor.execute(CREATE_ATTACHMENT_METADATA_TABLE_STATEMENT)
    cursor.execute(CREATE_GP2GP_MESSAGES_TABLE_STATEMENT)

    attachments_data_file = input_data_dir_path / "attachment_metadata.csv"
    cursor.execute(load_attachment_metadata_statement(attachments_data_file))

    gp2gp_messages_data_file = input_data_dir_path / "gp2gp_messages.csv"
    cursor.execute(load_gp2gp_messages_statement(gp2gp_messages_data_file))


def main():
    input_data_dir = argv[1]
    database_file = argv[2]
    cursor = duckdb.connect(database_file)
    construct_attachments_db(cursor, input_data_dir)
    cursor.close()


if __name__ == "__main__":
    main()
