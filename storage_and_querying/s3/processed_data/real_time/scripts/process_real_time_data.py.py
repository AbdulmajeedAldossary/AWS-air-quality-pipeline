import json
import base64
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    output = []
    for record in event['records']:
        payload = json.loads(base64.b64decode(record['data']).decode('utf-8'))
        logger.info(f"Decoded payload: {json.dumps(payload)}")

        unix_timestamp = payload['list'][0]['dt']
        readable_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(unix_timestamp))
        payload['list'][0]['timestamp'] = readable_timestamp

        logger.info(f"Transformed payload: {json.dumps(payload)}")
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(payload).encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)

    logger.info(f"Output: {json.dumps(output)}")
    return {'records': output}
