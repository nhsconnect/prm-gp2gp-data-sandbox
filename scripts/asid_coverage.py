from pathlib import Path
from datetime import datetime, date
from dateutil.tz import tzutc
from sys import argv
import pandas as pd

def main(asid_lookup_file_path, message_senders_file_path):
    message_senders_df = pd.read_csv(message_senders_file_path, names=["ASID"], header=0)
    asid_lookup_df = pd.read_csv(asid_lookup_file_path)

    senders_set = set(message_senders_df["ASID"])
    mapping_set = set(asid_lookup_df["ASID"].astype(int))

    difference = senders_set - mapping_set
    percent_not_in_mapping = (len(difference)/len(message_senders_df["ASID"]))*100
    percent_in_mapping = 100-percent_not_in_mapping

    print(f"{round(percent_in_mapping, 2)}% of the ASIDs from the Splunk query are covered by the ASID lookup CSV")


if __name__ == "__main__":
    asid_lookup_file_path = argv[1]
    message_senders_file_path = argv[2]
    main(asid_lookup_file_path, message_senders_file_path)



