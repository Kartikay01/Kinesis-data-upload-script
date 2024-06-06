import boto3
import json
import random
from datetime import datetime, timezone
import time

# Initialize a Kinesis client with AWS SDK
aws_access_key_id = ''
aws_secret_access_key = ''
kinesis_client = boto3.client('kinesis', region_name='',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

stream_name = 'stream1'

# Generate and send the data
def send_data(partition_key):
    try:
        data = [
            {
                "Timestamp": str(datetime.now(timezone.utc)),
                "longitude": random.uniform(-180, 180),
                "latitude": random.uniform(-90, 90),
                "altitude": random.uniform(0, 100),
                "angle": random.uniform(0, 360),
                "satellite": random.randint(0, 10),
                "speed": random.uniform(0, 100),
                "ignition": random.choice([0, 1]),
                "movement": random.choice([0, 1]),
            },
            {
                "Timestamp": str(datetime.now(timezone.utc)),
                "longitude": random.uniform(-180, 180),
                "latitude": random.uniform(-90, 90),
                "altitude": random.uniform(0, 100),
                "angle": random.uniform(0, 360),
                "satellite": random.randint(0, 10),
                "speed": random.uniform(0, 100),
                "ignition": random.choice([0, 1]),
                "movement": random.choice([0, 1]),
            },
            {
                "Timestamp": str(datetime.now(timezone.utc)),
                "longitude": random.uniform(-180, 180),
                "latitude": random.uniform(-90, 90),
                "altitude": random.uniform(0, 100),
                "angle": random.uniform(0, 360),
                "satellite": random.randint(0, 10),
                "speed": random.uniform(0, 100),
                "ignition": random.choice([0, 1]),
                "movement": random.choice([0, 1]),
            },
            {
                "Timestamp": str(datetime.now(timezone.utc)),
                "longitude": random.uniform(-180, 180),
                "latitude": random.uniform(-90, 90),
                "altitude": random.uniform(0, 100),
                "angle": random.uniform(0, 360),
                "satellite": random.randint(0, 10),
                "speed": random.uniform(0, 100),
                "ignition": random.choice([0, 1]),
                "movement": random.choice([0, 1]),
            }
        ]
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=partition_key
        )
        print(f"Data sent to Kinesis Data Stream with partition key: {partition_key}")
    except Exception as e:
        print(f"Error sending data to Kinesis: {e}")

# Run for 1 minute, sending 2 records every second
end_time = time.time() + 10800  # 60 seconds from now
while time.time() < end_time:
    send_data('data1')
    # send_data('partition_key_2_B_67890')
    time.sleep(5)  # Wait for 1 second before sending the next pair of records
