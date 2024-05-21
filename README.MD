# POC ICEBERG TABLES IN SNOWFLAKE

## NEEDS

- snowflake account
- storage bucket

## STORAGE SETUP

- create storage location, end prepare URL, storage must be in the same region as snowflake account:
     use `azure` when on Azure instead of `https`
- Azure tenant ID can be found under : `Microsoft Entra ID` service

## SNOWFLAKE SETUP

- Create users and roles, dbs

```sql
CREATE WAREHOUSE iceberg_lab;

CREATE ROLE iceberg_lab;
CREATE DATABASE iceberg_lab;
CREATE SCHEMA iceberg_lab;

GRANT ALL ON DATABASE iceberg_lab TO ROLE iceberg_lab WITH GRANT OPTION;
GRANT ALL ON SCHEMA iceberg_lab.iceberg_lab TO ROLE iceberg_lab WITH GRANT OPTION;;
GRANT ALL ON WAREHOUSE iceberg_lab TO ROLE iceberg_lab WITH GRANT OPTION;;

CREATE USER iceberg_lab
    PASSWORD='PASSWORD',
    LOGIN_NAME='ICEBERG_LAB',
    MUST_CHANGE_PASSWORD=FALSE,
    DISABLED=FALSE,
    DEFAULT_WAREHOUSE='ICEBERG_LAB',
    DEFAULT_NAMESPACE='ICEBERG_LAB.ICEBERG_LAB',
    DEFAULT_ROLE='ICEBERG_LAB';

GRANT ROLE iceberg_lab TO USER iceberg_lab;
GRANT ROLE iceberg_lab TO USER mateusz;
GRANT ROLE accountadmin TO USER iceberg_lab;
```
 
- Create external volume:

```sql
CREATE EXTERNAL VOLUME exvol
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'snowflake-icegerg-lab'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://something/something/'
        AZURE_TENANT_ID = 'ID'
      )
    );

GRANT ALL ON EXTERNAL VOLUME exvol TO ROLE iceberg_lab WITH GRANT OPTION;
```

- Get details needed for setting up cloud access:

```sql
DESC EXTERNAL VOLUME exvol;
```
```json
    {
        "NAME":"snowflake-icegerg-lab",
        "STORAGE_PROVIDER":"AZURE",
        "STORAGE_BASE_URL":"'azure://something/something/'",
        "STORAGE_ALLOWED_LOCATIONS":["'azure://something/something/'*"],"AZURE_TENANT_ID":"IDIDIDID",
        "AZURE_MULTI_TENANT_APP_NAME":"blabal_12344567",
        "AZURE_CONSENT_URL":"A URL",
        "ENCRYPTION_TYPE":"NONE",
        "ENCRYPTION_KMS_KEY_ID":""
        }
```

## CONFIGURE ACCESS ON THE CLOUD PROVIDER SIDE

- Go to `AZURE_CONSENT_URL` page. After accepting it will go to `snowflake.com`
- Go to Azure Portal ->> Storage Accounts, access must be granted on the Storage Account Level.
- Access Control(AIM) ->> Add role Assignment ->> Grant role `Storage Blob Data Contributor` to service principal from `AZURE_MULTI_TENANT_APP_NAME` value (**it can take an hour before tenant app is visible in IAM, serach using only the part of the name before _ sign**)  

## CREATE ICEBERG TABLE

```sql
USE ROLE iceberg_lab;
USE DATABASE iceberg_lab;
USE SCHEMA iceberg_lab;

CREATE OR REPLACE ICEBERG TABLE customer_iceberg (
    c_custkey INTEGER,
    c_name STRING,
    c_address STRING,
    c_nationkey INTEGER,
    c_phone STRING,
    c_acctbal INTEGER,
    c_mktsegment STRING,
    c_comment STRING
)  
    CATALOG='SNOWFLAKE'
    EXTERNAL_VOLUME='exvol'
    BASE_LOCATION='customer_iceberg' -- folder in which table will be created if not provided random value will be used
    ; 
```
