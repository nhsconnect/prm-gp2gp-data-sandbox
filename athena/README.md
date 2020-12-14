# AWS Athena

SQL queries to help validate the quality of the MI data stored in S3 bucket.

- [Count unique ODS codes in full month data](count_unique_ods_codes.txt) : Counts all unique ODS codes in HR (Header Record) in reporting periods 6-10 (February 2020) for TPP.
- [Create view with unique ODS codes in full month data](create_view_with_unique_ods_codes.txt) : Creates a view with all unique ODS codes in HR (Header Record) in reporting periods 6-10 (February 2020) for TPP.
- [Compare unique full month ODS codes with a single reporting period ODS codes](compare_unique_full_month_ods_codes.txt) : Joins and counts unique full month ODS codes with unique ODS codes in a one week reporting period for TPP. This validates that each practice sends records weekly regardless of whether they have transfers or not. The number of matching ODS codes should be the same as the number of unique full month ODS codes.
