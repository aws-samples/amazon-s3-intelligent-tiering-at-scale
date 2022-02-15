import json
import os
import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO,format='%(levelname)s: %(asctime)s: %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_tag_key = "storage.class"
bucket_tag_value = "s3.it"

def lambda_handler(event,context):
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
    lifecycle_config_settings_it = {
    'Rules': [
        {'ID': 'S3 Intelligent Tier Transition Rule',
         'Filter': {'Prefix': ''},
         'Status': 'Enabled',
         'Transitions': [
             {'Days': 0,
              'StorageClass': 'INTELLIGENT_TIERING'}
         ]}
    ]}

    archive_policy = {
        'Id': 'Archive_Tier', 
        'Status': 'Enabled',
        'Tierings': [
            {
                'Days': 90,
                'AccessTier': 'ARCHIVE_ACCESS'
            }, 
            {
                'Days': 180,
                'AccessTier': 'DEEP_ARCHIVE_ACCESS'
            }
        ]
    }
    ID = ('Archive_Tier')
    bucket_tag_key = "storage.class"
    bucket_tag_value = "s3.it"

    bucketName=event['detail']['requestParameters']['bucketName']
    logging.info(f'bucket name = {bucketName}')
    try:
        logging.info(f'bucket name = {bucketName}')
        tag_set = s3_resource.BucketTagging(bucketName).tag_set
        for tag in tag_set:
            tag_values = list(tag.values())
            logging.info(f'tag key ={tag["Key"]}' )
            logging.info(f'tag value ={tag["Value"]}' )
            if (tag['Key'] == bucket_tag_key):
                if(tag['Value'] == bucket_tag_value):
                    logging.info (f'TAG Match: tag key={bucket_tag_key} and tag value={bucket_tag_value} for bucket {bucketName}' )
                    put_bucket_lifecycle_configuration(bucketName,lifecycle_config_settings_it )  
                    put_bucket_intelligent_tiering_configuration(bucketName, archive_policy, ID)
    except ClientError as e:
        logging.info(f'No Tags')

    logger.info(f'S3 Bucket created  event handled OK')


def put_bucket_lifecycle_configuration(bucket_name, lifecycle_config):
    try:
        ok = s3_client.put_bucket_lifecycle_configuration(Bucket=bucket_name,
                                              LifecycleConfiguration=lifecycle_config)
        if ok:
            logger.info(f'The lifecycle configuration was set for {bucket_name}')
        else:
            logger.error(f'Could not set lifecycle configuration  for {bucket_name}')
                                              
    except ClientError as e:
        logger.error(e)
        return False
    return True

def put_bucket_intelligent_tiering_configuration(bucket_name, archive_policy, id):
    """Set the archive tier policy for an Amazon S3 bucket
    :param bucket_name: string
    :param intel_config: dict of intelligence tier configuration settings
    :param Id = Archive ID
    :return: True if archive policy configuration was set, otherwise False
    """
    try:
        ok = s3_client.put_bucket_intelligent_tiering_configuration(Bucket=bucket_name,
                                              IntelligentTieringConfiguration=archive_policy, Id=id)
        if ok:
            logging.info(f'The archive configuration was set for {bucket_name}')
        else:
            logger.error(f'Could not set archive configuration  for {bucket_name}')
            
    except ClientError as e:
        logging.error(e)
        return False
    return True