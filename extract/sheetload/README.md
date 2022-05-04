## SheetLoad - Spreadsheet & CSV Loader Utility

<img src="https://gitlab.com/meltano/analytics/uploads/d90d572dbc2b1b2c32ce987d581314da/sheetload_logo.png" alt="SheetLoadLogo" width="600"/>

Google Sheets and CSVs can be loaded into the data warehouse using `extract/sheetload/sheetload.py`. CSVs stored in GCS or S3 can be uploaded. A Google Sheet or S3-stored CSV will only be loaded if there has been a change between the current and existing data in the warehouse (unless otherwise specified). CSV files stored in GCS will always force a full reload.

If you are unsure what SheetLoad is or are requesting the inclusion of a data source into Snowflake/Periscope, please see the handbook page [Using SheetLoad](https://about.gitlab.com/handbook/business-ops/data-team/platform/#using-sheetload) and the corresponding [Issue Template](https://gitlab.com/gitlab-data/analytics/blob/master/.gitlab/issue_templates/CSV%20or%20GSheets%20Data%20Upload.md).

##### All commands should be run from within a `data-image` container. Run `make data-image` from the top-level of the `analytics` repo to use one.

Loading a Google Sheet:

  - Share the sheet with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
  - Files are stored in either the [Sheetload or Boneyard folders](https://drive.google.com/open?id=1F5jKClNEsQstngbrh3UYVzoHAqPTf-l0)
  - All sheets will be uploaded to either the `raw.sheetload` schema or `analytics.boneyard` schema depending on the file name
  - The file will be located and loaded based on its name. The names of the sheets shared with the runner must be unique
  - Add new sheets to config.yml file
  - Run the command `python3 sheetload.py sheets <path/to/yml/file>`
  - Logging from the script will tell you table successes/failures and the number of rows uploaded to each table.

 
Loading a CSV from GCS:

  - Put it in a bucket in the gitlab-analysis project
  - Make sure that the runner account has access (it should by default)
  - The name of the table will be the first part of the file name, for instance `data.csv.gz` will be in the table `data`
  - Run the command `python sheetload.py gcs --bucket <bucket_name> <file_name1> <file_name2> ...`
  - This command has a default param: `compression` (what compression is used) = `gzip`
  - Sheetload will then download the file and iterate through it, 15000 records at a time and upload them. 

Loading a CSV from S3:

 - Put CSVs in a bucket for which you have the access key and secret access key
 - Provide the bucket name and the schema name to sheetload
 - The name of the table will be the first part of the file name, for instance `users.csv` will be in the table `users`
 - AWS credentials are based on the schema name
 - Run the command `python sheetload.py s3 --bucket <bucket_name> --schema <schema_name>`
 - Sheetload will then iterate through each file in the bucket, download each, drop any existing table, and then upload 15000 records at a time for each file

Further Usage Help:

  - See [the sheetload dag](https://gitlab.com/gitlab-data/analytics/blob/master/dags/extract/sheetload.py) for a real world example of usage
  - Run the following command(s) for additional usage info `python3 sheetload.py <csv|sheets|gcs> -- --help`


### Driveload 

Loading a CSV from any Google Drive. 

 - Create a Google Drive Folder, duplicate folder names can be a problem so try and name the folder something unique. 
 - Share **folder** with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
 - Add the new folder and table name to the `drives.yml` file. 
   - If append is set to 1, the process will append the data to the existing table. After doing this it will create a folder called `Archived` and move the files here after processing. To add more data new files need to be dropped in the directory. This can cause duplicates if the same file is placed in the folder twice. 
   - If append is set to 0 the data will be read in from the file and the raw table will be replaced with the data from the file, similar to how the existing sheetload process operates.
 - After adding the table information the table will be created in the RAW database in the next run of the [the driveload dag](https://gitlab.com/gitlab-data/analytics/blob/master/dags/extract/driveload.py)

### Behavior

Read more [in the handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/#using-sheetload).

