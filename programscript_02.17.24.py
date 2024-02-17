# -*- coding: utf-8 -*-
"""
Created on 2/17/24

@author: asibl
"""

###############################################################################
# GLOBAL IMPORTS                                                              #
###############################################################################

#import base64
#import json
import os
#from urllib import request, parse
import boto3
import csv
from io import StringIO
from datetime import datetime, timedelta
from twilio.rest import Client

###############################################################################
# GETTING CONTENT FROM S                                                      #
###############################################################################

# Pulls in a .csv file from the S3 server
def get_csv_content(bucket_name, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    csv_content = response['Body'].read().decode('utf-8')
    csv_reader = csv.reader(StringIO(csv_content))
    return csv_reader

# Formats the csv as a list of rows ready to be used
def return_content(bucket_name, key):
    message_pool = []
    contents = get_csv_content(bucket_name, key)
    for row in contents: 
        message_pool.append(row)
    return message_pool


###############################################################################
# SENDING CONTENT BACK TO S3                                                  #
###############################################################################
def write_df_to_csv_and_send_to_s3(data_list, bucketname, key):
    # Convert list to CSV format
    csv_data = StringIO()
    csv_writer = csv.writer(csv_data)
    csv_writer.writerows(data_list)

    # Upload CSV data to S3
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_data.getvalue(), Bucket=bucketname, Key=key)
###############################################################################
# MESSAGING                                                                   #
###############################################################################

# Connects to API
def twilio_account_verification():
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    client = Client(account_sid, auth_token)
    return client

# Sends a message
def send_sms(twilio_client, out_number, message):
    message_to_send = twilio_client.messages.create(
        to=out_number,
        from_='+18887017141',
        body=message)
    print(message_to_send.sid)
    
# Morning message send
def morning_message_send(times_and_numbers, current_time, message_library, client, bucketname):
    print("It's morning!")
    for row in times_and_numbers:
    # only if their index <= 56
        scheduled_time = row[1]
        # tailoring/personalization
        name = row[4]
        pwud0 = row[5]
        pwud1 = row[6]
        pwud2 = row[7]
        pwud3 = row[8]
        pwud4 = row[9]
        pwud5 = row[10]
        pwud6 = row[11]
        pwud7 = row[12]
        subrate = row[13]
        stig1 = row[14]
        stig2 = row[15]
        stig3 = row[16]
        values = row[17]
        proudof = row[18]
        song = row[19]
        tailor = {'[NAME]': name, '[PWUD0]': pwud0, '[PWUD1]': pwud1, '[PWUD2]': pwud2, '[PWUD3]': pwud3, '[PWUD4]': pwud4, '[PWUD5]': pwud5, '[PWUD6]': pwud6, '[PWUD7]': pwud7, '[SUBRATE]': subrate, '[STIG1]': stig1, '[STIG2]': stig2, '[STIG3]': stig3, '[VALUES]': values, '[PROUDOF]': proudof, '[SONG]': song}
        #send message if time = current time
        if scheduled_time == current_time:
            phone_number = row[0]
            index = row[3]
            row[3] = int(row[3]) + 1
            for row in message_library:
                if row[1] == index:
                    message_to_send = row[0]
            # tailor the message
            for key, value in tailor.items():
                message_to_send = message_to_send.replace(key, value)
            send_sms(client, phone_number, message_to_send)
        else:
            continue
    write_df_to_csv_and_send_to_s3(times_and_numbers, bucketname, 'times_and_numbers.csv')

# Evening message send
def evening_message_send(times_and_numbers, current_time, message_library, client, bucketname):
    print("It's evening!")
    for row in times_and_numbers:
        print(row)
        scheduled_time = row[2]
        # tailoring/personalization
        name = row[4]
        pwud0 = row[5]
        pwud1 = row[6]
        pwud2 = row[7]
        pwud3 = row[8]
        pwud4 = row[9]
        pwud5 = row[10]
        pwud6 = row[11]
        pwud7 = row[12]
        subrate = row[13]
        stig1 = row[14]
        stig2 = row[15]
        stig3 = row[16]
        values = row[17]
        proudof = row[18]
        song = row[19]
        tailor = {'[NAME]': name, '[PWUD0]': pwud0, '[PWUD1]': pwud1, '[PWUD2]': pwud2, '[PWUD3]': pwud3, '[PWUD4]': pwud4, '[PWUD5]': pwud5, '[PWUD6]': pwud6, '[PWUD7]': pwud7, '[SUBRATE]': subrate, '[STIG1]': stig1, '[STIG2]': stig2, '[STIG3]': stig3, '[VALUES]': values, '[PROUDOF]': proudof, '[SONG]': song}
        if scheduled_time == current_time:
            phone_number = row[0]
            index = row[3]
            row[3] = int(row[3]) + 1
            for row in message_library:
                if row[1] == index:
                    message_to_send = row[0]
            # tailor the message
            for key, value in tailor.items():
                message_to_send = message_to_send.replace(key, value)
            send_sms(client, phone_number, message_to_send)
        else:
            continue
    write_df_to_csv_and_send_to_s3(times_and_numbers, bucketname, 'times_and_numbers.csv')

# PME Qualtrics

def pme_trigger(times_and_numbers, client, bucketname):
    links = {1: "https://go.unc.edu/RESTART1", 2: "link2", 3: "link3", 4: "link4"}
    pme_triggers = {"14": "1", "28": "2", "42": "3", "56": "4"}
    for row in times_and_numbers:
        pme_message = "Hey! Can you take a second and let us know what you thought of this week's messages? [LINK]"
        pme_index = row[15]
        index = row[3]
        phone_number = row[0]
        for key, value in pme_triggers.items():
            if key == index and value == pme_index:
                print("true!")
                pme_message = pme_message.replace("[LINK]", links[int(pme_index)])
                row[15] = int(row[15]) + 1
                send_sms(client, phone_number, pme_message)
            else:
                continue
    write_df_to_csv_and_send_to_s3(times_and_numbers, bucketname, 'times_and_numbers.csv')
        
                

# get current time in string and current hour in int
def get_times():
    current_time = datetime.now() - timedelta(hours=5)
    current_time_str = current_time.strftime("%H:%M")
    current_hour = current_time.hour
    return current_time_str, current_hour
    

###############################################################################
# MAIN FUNCTION                                                               #
###############################################################################

def lambda_handler(event, context):
    
    twilio_client = twilio_account_verification()
    
    # .csvs are accessed by the S3 bucket and filename key
    bucketname = 'alsdissertation'
    
    # pull in numbers, preferred times, index
    times_and_numbers_df = return_content(bucketname, 'times_and_numbers.csv')
    # pull in messages
    message_library = return_content(bucketname, 'messages.csv')
    # make variables of current times
    current_time_str, current_hour = get_times()
    
    # check if morning
    if 6 <= current_hour <= 12:
        morning_message_send(times_and_numbers_df, current_time_str, message_library, twilio_client, bucketname)
    
    #check if evening        
    elif 13 <= current_hour <= 23:
        pme_trigger(times_and_numbers_df, twilio_client, bucketname)
        evening_message_send(times_and_numbers_df, current_time_str, message_library, twilio_client, bucketname)
