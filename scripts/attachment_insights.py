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
            CASE
                WHEN compressed='Yes' THEN TRUE
                WHEN compressed='No' THEN FALSE
                ELSE NULL END
            as compressed,
            contentType as content_type,
            CASE
                WHEN largeAttachment='Yes' THEN TRUE
                WHEN largeAttachment='No' THEN FALSE
                ELSE NULL END
            as large_attachment,
            CASE
                WHEN length='Unknown' THEN NULL
                ELSE CAST(length AS INT) END
            as length,
            CASE
                WHEN originalBase64='Yes' THEN TRUE
                WHEN originalBase64='No' THEN FALSE
                ELSE NULL END
            as original_base64
        FROM read_csv_auto('{filename}', all_varchar=TRUE);
    """


def main():
    input_data_dir = Path(argv[1])
    database_file = argv[2]

    attachments_data_files = input_data_dir.rglob("*csv")

    cursor = duckdb.connect(database_file)

    cursor.execute(CREATE_ATTACHMENT_METADATA_TABLE_STATEMENT)

    for data_file in attachments_data_files:
        print(f"Loading {data_file}")
        cursor.execute(load_attachment_metadata_statement(data_file))

    print("Done")
    cursor.close()


if __name__ == "__main__":
    main()
