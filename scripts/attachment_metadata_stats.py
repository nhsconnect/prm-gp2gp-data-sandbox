import duckdb
from sys import argv

from bokeh.io import output_file, show
from bokeh.plotting import figure

# These can be run after running the attachment_insights script
def create_view_attachments_count_per_conversation(attachment_metadata):
    return attachment_metadata\
        .aggregate("conversation_id, count(*) AS number_attachments")\
        .order("number_attachments desc")\
        .create_view("attachments_per_conversation")


def create_view_attachment_count_frequency(attachments_per_conversation):
    return attachments_per_conversation \
        .aggregate("number_attachments, count(*) AS transfer_count") \
        .order("number_attachments desc") \
        .create_view("number_attachment_frequency")


def main():
    attachment_metadata_db_path = argv[1]
    cursor = duckdb.connect(attachment_metadata_db_path)
    attachment_metadata = cursor.table("attachment_metadata")
    attachment_count_per_conversation = create_view_attachments_count_per_conversation(attachment_metadata)
    attachment_count_frequency = create_view_attachment_count_frequency(attachment_count_per_conversation)

    #TODO: look at attchment ids
    #TODO: what about records with no attachments

    frequency_counts = attachment_count_frequency.execute().fetchall()

    num_attachments = [row[0] for row in frequency_counts]
    transfer_counts = [row[1] for row in frequency_counts]

    output_file("bokehtest.html")
    p = figure(plot_width=800, plot_height=400, title="Number of attachments distribution")
    p.vbar(x=num_attachments, width=0.5, top=transfer_counts, bottom=0)
    show(p)

    print(f"Most number of attachments per conversation: {attachment_count_per_conversation.limit(3)}")
    print(f"Most frequent number of attachments associated in a transfer: {attachment_count_frequency.limit(3)}")


if __name__ == "__main__":
    main()
