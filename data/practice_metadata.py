import boto3, json, sys
import pandas as pd


def warn(message):
    print(f"Warning: {message}", file=sys.stderr)

    
def read_json(s3_object):
    return json.loads(s3_object.get()['Body'].read().decode('utf-8'))

    
def flatten_practice_asids(ods_metadata):
     return pd.DataFrame([
        {
            'asid': asid,
            'practice_ods_code': practice['ods_code'],
            'practice_name': practice['name']
        }
        for practice in ods_metadata["practices"]
        for asid in practice['asids']
    ])
     

def flatten_practice_ccgs(ods_metadata):
    return pd.DataFrame([
        {
            'practice_ods_code': practice_ods_code,
            'ccg_ods_code': ccg['ods_code'],
            'ccg_name':ccg['name']}
        for ccg in ods_metadata['ccgs']
        for practice_ods_code in ccg['practices']
    ])


def check_each_asid_only_has_one_practice(practice_asids):
    if practice_asids['asid'].duplicated().any():
        warn("At least one asid appears to have more than one practice")

        
def check_each_practice_only_has_one_ccg(practice_ccgs):
    if practice_ccgs['practice_ods_code'].duplicated().any():
        warn("At least one practice appears to have more than one CCG")

        
def check_all_asids_have_ccg_metadata(asid_lookup):    
    if any(asid_lookup["ccg_name"].isna()):
        warn("At least one practice appears to be missing CCG information")
        

def build_asid_lookup(practice_asids, practice_ccgs):
    asids = practice_asids.set_index('asid')
    ccgs = practice_ccgs.set_index('practice_ods_code')
    return asids.join(ccgs, on='practice_ods_code', how='left')


def read_asid_metadata(bucket_name, key):
    s3 = boto3.resource("s3")
    ods_s3_object = s3.Object(bucket_name, key)
    ods_metadata = read_json(ods_s3_object)
    practice_asids = flatten_practice_asids(ods_metadata)
    practice_ccgs = flatten_practice_ccgs(ods_metadata)

    check_each_asid_only_has_one_practice(practice_asids)
    check_each_practice_only_has_one_ccg(practice_ccgs)

    asid_lookup = build_asid_lookup(practice_asids, practice_ccgs)
    check_all_asids_have_ccg_metadata(asid_lookup) 
    
    return asid_lookup
