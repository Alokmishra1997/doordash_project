import json
import boto3
import pandas as pd 
from io import StringIO
 
def lambda_handler(event, context):
    # Initialize SNS client
    sns_client = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-east-1:471112764661:my_sns_topic'
    
     
    print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print("bucket:", bucket)
    print("key:", key)
    #s3://doordashing-landing-zn/2024-03-09-raw_input.json
    print("##################################")
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    print("response :",response)
    file_content = response["Body"].read().decode('utf-8')
    print("file_content : ", file_content)
    
    print(type(file_content))

# Split the content by newline to get each JSON object
#
    json_objects = [json.loads(line) for line in file_content.split('\n') if line.strip()]
 
    print("***************************************************")
    df = pd.DataFrame(json_objects)
    print("Data :",json_objects)
    df1 = df[df["status"] == "delivered"]
    print("DataFrame is  : " ,df1)
     
    json_buffer = StringIO()
    df1.to_json(json_buffer, orient='records') 
    
    # Define the target S3 bucket and file name
    target_bucket_name = 'doordash-target-zn-1234'
    target_file_name = 'data.json'
     
    s3_client.put_object(Bucket=target_bucket_name, Key=target_file_name, Body=json_buffer.getvalue())
    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    print("DataFrame successfully written to S3")
        
        # Publish success message to SNS
    message = 'DataFrame has been processed successfully'
    response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject='SUCCESS - Daily Data Processing'
        )
        
    '''
    except Exception as err:
        print(err)
        message = "DataFrame processing failed: {}".format(str(err))
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject='FAILED - Daily Data Processing'
        )
    '''
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }