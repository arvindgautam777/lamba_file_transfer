# pylint: disable=missing-module-docstring
from downloader import *

def lambda_handler(event, context):

    #setting the flg to true to start the search process
    upload_flag = True

    while upload_flag:
        #get all file names from website
        file_names = get_list_of_files()
        
        #get list of files from the s3 bucket
        s3_bucket_files = list_of_files_in_s3_bucket(bucket)

        #for every file in website, search the file in s3 bucket
        for file in file_names:
            #if the selected file is not present in bucket
            print(s3_bucket_files)
            if file not in s3_bucket_files:
                print(f"Initiating transfer proccess for file {file}")
                transfer_res = transfer_file_from_website_to_s3(file)
                upload_flag = False
                break
            else:
                print("looking for next file")


    return transfer_res