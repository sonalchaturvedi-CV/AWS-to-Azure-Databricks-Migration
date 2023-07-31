import boto3

# Create a Glue client
glue_client = boto3.client('glue')

# Specify the database name and new location path
database_name = 'nyctaxiyellowdb'
new_location = 's3://aws-glue-yellowtaxidata/Database/'

# Get the current database properties
database_response = glue_client.get_database(Name=database_name)
database = database_response['Database']

# Remove the CreateTime and CatalogId parameters
database.pop('CreateTime', None)
database.pop('CatalogId', None)

# Update the location of the database
database['LocationUri'] = new_location

# Update the database properties
glue_client.update_database(Name=database_name, DatabaseInput=database)

