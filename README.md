# AWS-to-Azure-Databricks-Migration

    Description
    Upload Files to AWS S3
    Create EMR Cluster
    Create IAM Role
    Add Data to Glue Catalog Table Using Crawler
    EMR For Creating Partitions
    Migrate Table Data from AWS to Azure Databricks Using AWS Glue
    Migrate Table Data from AWS to Azure Blob Storage Using Azure’s “azcopy”
    Check Migration and Create Delta Table

#**Description**
    The idea of this POC is to use:
    1. AWS EMR: Creating a table in Glue Catalog and creating partitions. 
    2. AWS Glue: Create a glue job to transfer data from this table to a table on Databricks using JDBC connection. (One way of migration)
    3. Azure’s azcopy feature: Copy table along with its metadata to Azure blob storage. (Second way of migration)
    4. Databricks Notebook: Create table in delta lake and copy data by inferring schema from Azure Blob Storage. 
      For this, the plan is to manually trigger an EMR cluster to upload a file into Glue data catalog from S3 bucket and also simultaneously trigger an AWS Glue Job using 
      JDBC connection. 




#**Upload Files to AWS S3**
    1. Create an s3 bucket
    2. Upload your file to s3 bucket
    3. For this POC, I took one year worth of data from this website
    4. Create EMR Cluster
    5. Go to AWS EMR service and click create a new cluster.
    6. Give a name to your cluster.
    7. Select “Spark” from the application bundle.
    8. Enable “Use for Spark table metadata” under “AWS Glue Data Catalog settings”
    9. Under “Cluster configuration”, select default configurations or configure based on your use case.  
    10. Create a KeyPair so that you can access EC2 (your EMR cluster) from terminal or other locations. 
    11. Create IAM Role
        One EMR role that should have:
          -read and write access to s3
          -access to Glue Console
          -aws managed EMR policy
        One Glue role that should have:
          -aws managed glue service policy
          -read and write access to s3
          -policy with Glue’s GetTable, CreateTable, GetDatabase actions and iam’s PassRole actions.


#**Add Data to Glue Catalog Table Using Crawler**
    1. Create Database
    2. Click Databases under Glue Catalog
    3. Under tables, click on “Add tables using crawler”
    4. Configure settings under “Data source configuration”. Add data source (S3 in this case). Add s3 path and all other required fields. Click Next.
    5. Add Glue IAM role to this (the one we configured earlier), a new role can also be created at this step.  
    6. Select a target database (the database that we created)
    7. Review the configurations
    10. Create crawler
    11. Once the crawler is created, click “Run crawler”
    12. The status would change to “Completed”. After that you can see table details under “Table changes”
    13. Check under databases to see if your table has been created. 

#**EMR For Creating Partitions**
Cluster creation might take a while.
    1. Once cluster is created, click on “Connect to the Primary Node using SSH”
    2. Follow the steps to connect to the cluster using Terminal (if MAC)
    3. Ssh into the instance using steps mentioned above
    4. Use the scp command to copy your script from the local machine to this ec2 machine. 
    5. The scp command would look like this:
        scp -i ~/path/to/your_key.pem ~/path/to/scripts/script.py hadoop@instance :~/script.py
    6. Run this script using spark.submit command. The command would look like this:
        spark-submit --master yarn /home/hadoop/script.py
    7. My requirement was to create a partition on the month and year columns. 
The script can be found here.

Now we have partitioned data ready.

#**Migrate Table Data from AWS to Azure Databricks Using AWS Glue**
    1. Upload a JAR file which contains JDBC connection for Databricks to S3 bucket. In this POC, the driver I used is named “DatabricksJDBC42.jar”
    2. Create a Glue Connector:
    3. Choose “Create Custom Connector”
    4. Add className (“com.databricks.client.jdbc.Driver”). Look up for the correct className in the guide that comes with the driver. 
    5. Add an S3 path where your driver (jar file) is stored. 
    6. Add JDBC Url (This would be copied from Databricks -> SQL Warehouse -> Select your warehouse -> Connection details -> Copy JDBC Url from here -> Also generate a     
       private 
    7. token and use it as a password in PWD parameter in JDBC Url)
    8. Create connector
    9. Create a Glue Connection utilizing this connector
    10. Name your connection
    11. Select “default” connection type and verify JDBC Url
    12. Create connection
    13. Now create an ETL job with source as Glue DataCatalog and target as the connection that we just created
    14. Check the Glue Script, compare with this script
    15. Run the job
    16. If the job fails, you might need to create a table with schema in your target database (on Databricks) first Check Databricks database and table to see if data has 
        been copied here

#**Migrate Table Data from AWS to Azure Blob Storage Using Azure’s “azcopy”**
1. Export aws access id and key using export command. The commands would look like this: 
    export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXX
    export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXX
2. Use “azcopy” command to copy your entire directory from s3 to azure blob storage. The command would look like this:
    azcopy copy source s3-url-here" "target blob-storage-url-here" --recursive=true

#**Check Migration and Create Delta Table**
1. Mount blob storage to Databricks File System
2. Read your file from the dbfs file path
3. Write in delta format and save as new table
4. Check if data is loaded perfectly
5. All codes are logged here
