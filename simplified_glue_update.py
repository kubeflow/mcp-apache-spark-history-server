import boto3
import logging

def update_glue_catalog_external(spark, create_ddl, glue_database, glue_table, is_partition_required="no"):
    """Simplified Glue catalog update with cross-account access"""
    
    # Setup cross-account Glue client
    glue_client = get_cross_account_glue_client()
    
    # Parse DDL and convert to Glue format
    ddl_metadata = parse_ddl_to_glue_format(spark, create_ddl, glue_table)
    
    # Upsert table metadata
    glue_upsert_metadata(glue_client, glue_database, glue_table, ddl_metadata)
    
    # Handle partitions if needed
    if is_partition_required.lower() == "yes":
        handle_partitions(spark, glue_client, glue_database, glue_table)

def get_cross_account_glue_client():
    """Create Glue client with cross-account role"""
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn='arn:aws:iam::750557897806:role/GlueCrossAccountUpdateRole',
        RoleSessionName='cross_account_glue_update'
    )
    
    return boto3.client('glue',
        aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
        aws_session_token=assumed_role['Credentials']['SessionToken']
    )

def parse_ddl_to_glue_format(spark, create_ddl, table_name):
    """Parse Spark DDL to Glue table format"""
    table_props = spark._jsparkSession.sessionState().sqlParser().parsePlan(create_ddl).toCreateTable()
    
    return {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': [{"Name": f.name(), "Type": f.dataType().simpleString()} for f in table_props.schema()],
            'Location': table_props.location(),
            'InputFormat': table_props.storage().inputFormat().orNull(),
            'OutputFormat': table_props.storage().outputFormat().orNull(),
            'SerdeInfo': {
                'SerializationLibrary': table_props.storage().serde().orNull(),
                'Parameters': dict(table_props.storage().properties())
            }
        },
        'PartitionKeys': [{"Name": f.name(), "Type": f.dataType().simpleString()} for f in table_props.partitionSchema()],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': dict(table_props.properties())
    }

def glue_upsert_metadata(glue_client, database_name, table_name, ddl_metadata):
    """Upsert table metadata - create if not exists, update if exists"""
    verify_create_database(glue_client, database_name)
    
    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
        glue_client.update_table(DatabaseName=database_name, TableInput=ddl_metadata)
        logging.info(f"Updated table {database_name}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database_name, TableInput=ddl_metadata)
        logging.info(f"Created table {database_name}.{table_name}")

def verify_create_database(glue_client, database_name):
    """Create database if it doesn't exist"""
    try:
        glue_client.get_database(Name=database_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(DatabaseInput={"Name": database_name})
        logging.info(f"Created database {database_name}")

def handle_partitions(spark, glue_client, database_name, table_name):
    """Handle partition repair and crawler"""
    spark.sql(f"MSCK REPAIR TABLE {database_name}.{table_name}")
    
    try:
        crawler_name = f"{database_name}_{table_name}_crawler"
        glue_client.start_crawler(Name=crawler_name)
    except Exception as e:
        logging.warning(f"Crawler start failed: {e}")
