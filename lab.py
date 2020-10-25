'''
Boto3 documentation for DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#client

Remember: Run 'python -m pip install boto3 --user' on Cloud9.
'''

# Load the AWS SDK for Python
import boto3

# Load the exceptions for error handling
from botocore.exceptions import ClientError, ParamValidationError

# JSON handling
import json

# Create AWS service client and set region
db = boto3.client('dynamodb', region_name='us-east-1')

# Get a list of tables in region
def get_tables():
    try:
        data = db.list_tables()
        return data['TableNames']
    # An error occurred
    except ParamValidationError as e:
        print("Parameter validation error: %s" % e)
    except ClientError as e:
        print("Client error: %s" % e)
        
#Uploaded a file to to S3, downloading the same
s3 = boto3.client('s3', region_name='us-east-1')
def download_data():
    try:
        #get_object is an S3 function, Bucket and Key are required params. Refer docs
        data_object = s3.get_object(
            Bucket = "aws-dev-associate-dynamodb-test",
            Key = "lab-data/test-table-items.json"
            )
        data_string = data_object['Body'].read().decode('utf-8')
        # print(f"Downloaded from S3: {data_string}")
        data = json.loads(data_string)
        print(type(data))
        return data
        
    except ParamValidationError as e:
        print(f"Parameter validation error: {e}")
    except ClientError as e:
        print(f"Client error: {e}")
        
#Method to write downloaded data to the DynamoDB database. 
def write_dynamo_db(json_data):
    try:
        #batch_write_item is a DynamoDB method. 
        data = db.batch_write_item(
            RequestItems = json_data
            )
        #Response returned from batch_write_item = UnprocessedItems(dict)    
        print(f"Unprocessed items: {data['UnprocessedItems']}")
        return data
    
    except ParamValidationError as e:
        print(f"Parameter validation error: {e}")
    except ClientError as e:
        print(f"Client error: {e}")
        
#Method to query DynamoDB
def query_dynamo_db():
    try:
        #query is a DynamoDB method. Refer docs. TableName is required. 
        data = db.query(
            TableName = "test-table",
            IndexName = "ProductCategory-Price-index",
            ##:c and :p are placeholder tokens for the actual values.
            KeyConditionExpression = "ProductCategory = :c AND Price <= :p",
            ExpressionAttributeValues = {
                ':c': {'S': 'Bike'},
                ':p': {'N': '300'}
                } #Here we defined the actual values for the tokens defined above.
            )
        print("Items that matched the above condition:")
        #Response from the query request is 'Items(list of dicts)'
        for item in data["Items"]:
            print(item)

    except ParamValidationError as e:
        print(f"Parameter validation error: {e}")
    except ClientError as e:
        print(f"Client error: {e}")
        
# Main program
def main():
    table_names = get_tables()
    if (len(table_names)) == 0:
        print('No tables in region.')
    else:
        for x in table_names:
            print('Table name: '+ x )
            
        downloaded_data = download_data()
        
        # write_dynamo_db(downloaded_data)
        
        query_dynamo_db()
            
if __name__ == '__main__':
    main()
