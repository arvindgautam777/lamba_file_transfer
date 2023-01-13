# pylint: disable=no-name-in-module
from lambda_function import lambda_handler

res1 = lambda_handler(None, None)
#print("File download status ", res1.status_code)
print("Upload Status ", res1['ResponseMetadata']['HTTPStatusCode'])