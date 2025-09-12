import requests
import base64

from dotenv import load_dotenv
import os
import webbrowser
import json
from datetime import datetime, timezone
import time
import sys
import threading
import paho.mqtt.client as mqtt
import market_open





MINUTES_IN_A_WEEK = 10080
MINUTES_IN_A_HALF_HOUR = 30
MINUTES_IN_A_DAY = 1440


global appKey
appKey = None

global appSecret
appSecret = None

global access_token 
access_token = None

old_access_token = None

global refresh_token 
refresh_token = None

old_refresh_token = None

global id_token 
id_token = None

global acctNum
acctNum = None
global hashVal
hashVal = None

mri_tokens_file = None
mri_acct_file = None

global arg_one
arg_one = None

global gbl_mqtt_client
gbl_mqtt_client = None

global new_access_token_flag
new_access_token_flag = False










def load_env_variables():
    global mri_tokens_file
    global mri_acct_file
    
    # parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    # env_file_path = os.path.join(parent_dir, '.env')
    # load_dotenv(env_file_path)

    load_dotenv()  # load environment variables from .env file

    my_app_key = os.getenv('MY_APP_KEY')
    my_secret_key = os.getenv('MY_SECRET_KEY')
    my_tokens_file = os.getenv('TOKENS_FILE_PATH')
    mri_tokens_file  = os.getenv('MRI_TOKENS_FILE_PATH')
    mri_acct_file  = os.getenv('MRI_ACCT_FILE_PATH')

    # print(f'my_local_app_key: {app_key}, my_local_secret_key: {secret_key}')
    # print(f'tokens_file type: {type(tokens_file)}, value: {tokens_file}')

    return my_app_key, my_secret_key, my_tokens_file






# appKey, appSecret, tokensFile = load_env_variables()

# print(f'appKey:{appKey}, appSecret:{appSecret}')

# # appKey = 'Your app key'
# # appSecret = 'Your app secret'

# authUrl = f'https://api.schwabapi.com/v1/oauth/authorize?client_id={appKey}&redirect_uri=https://127.0.0.1'



# # Open the URL in the default web browser
# webbrowser.open(authUrl)
# print(f"Opening browser for authentication: {authUrl}")


# # print(f"Click to authenticate: {authUrl}")

# returnedLink = input("Paste the redirect URL here:")

# code = f"{returnedLink[returnedLink.index('code=')+5:returnedLink.index('%40')]}@"


# headers = {'Authorization': f'Basic {base64.b64encode(bytes(f"{appKey}:{appSecret}", "utf-8")).decode("utf-8")}', 'Content-Type': 'application/x-www-form-urlencoded'}
# data= {'grant_type': 'authorization_code', 'code': code, 'redirect_uri': 'https://127.0.0.1'}
# response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)
# tD = response.json()

# access_token = tD['access_token']
# refresh_token = tD['refresh_token']

# base_url = 'https://api.schwabapi.com/trader/v1/'

# response = requests.get(f'{base_url}/accounts/accountNumbers', headers={'Authorization': f'Bearer {access_token}'})
# print(response.json())




# Define global variables
access_token_issue_date = None
refresh_token_issue_date = None
expires_time = None
token_type = None
scope = None

transpired_access_minutes = None
transpired_refresh_minutes = None
access_minutes_left = None
refresh_minutes_left = None



streamer_socket_url = None
customer_id = None
correl_id = None
channel = None
function_id = None






def get_user_preferences(access_token):
    global streamer_socket_url
    global customer_id
    global correl_id
    global channel
    global function_id


    



    # print(f'\ngetting userPreference, access_token:{access_token}')

    # GET userPreference
    url = "https://api.schwabapi.com/trader/v1/userPreference"

    # Define the headers
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    # Make the GET request
    response = requests.get(url, headers=headers)

    # print(f'7011 userPreference:{response.status_code}')
    # print(f'7012 data:{response.json()}')


    # Display the results
    if response.status_code == 200:
        userPreference_data = response.json()
        # print(f'userPreference_data type:{type(userPreference_data)}, data:\n{userPreference_data}')


        # Extract streamerInfo details
        streamer_info = userPreference_data.get("streamerInfo", [{}])[0]

        # Assign extracted values to variables
        streamer_socket_url = streamer_info.get("streamerSocketUrl", "")
        customer_id = streamer_info.get("schwabClientCustomerId", "")
        correl_id = streamer_info.get("schwabClientCorrelId", "")
        channel = streamer_info.get("schwabClientChannel", "")
        function_id = streamer_info.get("schwabClientFunctionId", "")

        # # Display extracted values
        # print(f'\n482  userPreference settings:')
        # print(f"Streamer Socket URL: {streamer_socket_url}")
        # print(f"Schwab Client Customer ID: {customer_id}")
        # print(f"Schwab Client Correl ID: {correl_id}")
        # print(f"Schwab Client Channel: {channel}")
        # print(f"Schwab Client Function ID: {function_id}")


    else:
        print(f"309 userPreference Error {response.status_code}: {response.text}")
        raise







def read_tokens_mri_file():

    token_data = None

    try:
        # Open and read the JSON file
        with open(mri_tokens_file, "r") as f:
            token_data = json.load(f)

    except Exception as e:
        print(f"204 Error reading tokens file ({mri_tokens_file}) file: {e}")
        raise
        
    return token_data

def extract_tokens(tokens_object):

    pass





def read_acct_mri_file():

    acct_data = None

    try:
        # Open and read the JSON file
        with open(mri_acct_file, "r") as f:
            acct_data = json.load(f)

    except Exception as e:
        print(f"205 Error reading account file ({mri_tokens_file}) file: {e}")
        raise
        
    return acct_data


def extract_account_mri(my_acct_data):
    global acctNum
    global hashVal

    acctNum = None
    hashVal = None

    # print(f'933 my_acct_data type:{type(my_acct_data)}, data:{my_acct_data}')

    try:
        acctNum = my_acct_data['account_number']
        hashVal = my_acct_data['account_hash']

    except Exception as e:
        print(f"831 Error extracting account data: {e}")
        print(f'934 my_acct_data type:{type(my_acct_data)}, data:{my_acct_data}')


    


    


def all_tokens_initialized():
    global access_token_issue_date, refresh_token_issue_date
    global expires_time, token_type, scope
    global refresh_token, access_token, id_token
    global acctNum, hashVal

    if access_token_issue_date == None:
        print(f'ati access_token_issue_date is None')
    if refresh_token_issue_date == None:
        print(f'ati refresh_token_issue_date is None')
    if expires_time == None:
        print(f'ati expires_time is None')
    if token_type == None:
        print(f'ati token_type is None')
    if refresh_token == None:
        print(f'ati refresh_token is None')
    if access_token == None:
        print(f'ati access_token is None')
    if id_token == None:
        print(f'ati id_token is None')
    if acctNum == None:
        print(f'ati acctNum is None')
    if hashVal == None:
        print(f'ati hashVal is None')


    # Collect all global variables into a list
    variables = [
        access_token_issue_date, refresh_token_issue_date,
        expires_time, token_type, scope,
        refresh_token, access_token, id_token,
        acctNum, hashVal
    ]
    
    # Return True if none of them are None, otherwise False
    return all(var is not None for var in variables)



def refresh_token_minutes_left(token_file_data):

    # print(f'token file data type: {type(token_file_data)}, data:\n{token_file_data}')

    refresh_minutes_left = 0

    try:

        if 'refresh_token_issued' in token_file_data:

            refresh_timestamp = datetime.fromisoformat(token_file_data['refresh_token_issued'])

            now_utc = datetime.now(timezone.utc)
            time_delta = now_utc - refresh_timestamp

            days_delta = time_delta.days

            transpired_refresh_minutes = int(time_delta.total_seconds() // 60)

            refresh_minutes_left = MINUTES_IN_A_WEEK - transpired_refresh_minutes 
            days_left_fl = float(refresh_minutes_left / 1440)

            # print(f'days_delta:{days_delta}, transpired_refresh_minutes:{transpired_refresh_minutes}')
            # print(f'refresh_minutes_left:{refresh_minutes_left}, days_left_fl{days_left_fl:.2f}')

    except Exception as e:
        print(f"20030 Error getting refresh token status: {e}")
            

    return refresh_minutes_left








def extract_tokens_mri(token_data):
    global access_token_issue_date, refresh_token_issue_date
    global expires_time, token_type, scope
    global refresh_token, access_token, id_token
    global transpired_access_minutes, transpired_refresh_minutes
    global refresh_minutes_left, access_minutes_left


    # print(f'mri_tokens_file:{mri_tokens_file}')

    # try:
    #     # Open and read the JSON file
    #     with open(mri_tokens_file, "r") as f:
    #         token_data = json.load(f)

    # except Exception as e:
    #     print(f"110 Error opening file: {e}")
    #     return

    # Extract values
    access_token_issue_date = token_data.get("access_token_issued")

    # print(f'\n6670 access_token_issue_date type:{type(access_token_issue_date)}, value:{access_token_issue_date}\n')



    refresh_token_issue_date = token_data.get("refresh_token_issued")
    token_dict = token_data.get("token_dictionary", {})

    expires_time = token_dict.get("expires_in", 1800)  # Default to 1800 if missing
    token_type = token_dict.get("token_type", "Bearer")  # Default if missing
    scope = token_dict.get("scope", "api")  # Default if missing
    refresh_token = token_dict.get("refresh_token")
    access_token = token_dict.get("access_token")
    id_token = token_dict.get("id_token")


    need_new_access_token = False


    try:

        now = datetime.now(timezone.utc)

        refresh_dt = datetime.fromisoformat(refresh_token_issue_date)
        transpired_refresh_minutes = (int)((now - refresh_dt).total_seconds() / 60)
        refresh_minutes_left = MINUTES_IN_A_WEEK - transpired_refresh_minutes 
        refresh_days_left = refresh_minutes_left/1440

        print(f'transpired_refresh_minutes:{transpired_refresh_minutes}, minutes left:{refresh_minutes_left}, days_left:{refresh_days_left:.2f}')

        # print(f'access_token_issue_date:{access_token_issue_date}')
        access_dt = datetime.fromisoformat(access_token_issue_date)
        
        transpired_access_minutes = (int)((now - access_dt).total_seconds() / 60)
        access_minutes_left = MINUTES_IN_A_HALF_HOUR - transpired_access_minutes



        # print(f'\n\n%%%%%%%%%%%%%%%%%%%')
        # print(f'7720 access_minutes_left:{access_minutes_left}')

        if access_minutes_left <= 5:
        # if access_minutes_left <= 20:

            need_new_access_token = True
            print(f'3791 access_minutes_left:{access_minutes_left}, new access token needed')
        else:
            need_new_access_token = False

    except Exception as e:
        print(f"3733 Error getting calcuating access_minutes_left: {e}")
        need_new_access_token = True
        raise


    if need_new_access_token:
        print(f'3583 need to re-authorize,\n  access_token:{access_token}\n  refresh_token:{refresh_token}')

    
        try:
            token_dict = refresh_auth_token()
            # print(f'token_dict type{type(token_dict)}, data:\n{token_dict}')

            now_iso = datetime.now(timezone.utc).isoformat()

            # Extract token values from dictionary
            new_refresh_token = token_dict.get("refresh_token")
            new_access_token = token_dict.get("access_token")
            new_id_token = token_dict.get("id_token")

            # Display extracted values
            # print(f"New Refresh Token: {new_refresh_token}")
            print(f"0829 old access token:{access_token},  New Access Token: {new_access_token}")
            # print(f"New ID Token: {new_id_token}")

            access_token = new_access_token

            print(f'etm access_token:{access_token}')


            access_dt = datetime.fromisoformat(now_iso)
            transpired_access_minutes = (int)((now - access_dt).total_seconds() / 60)
            access_minutes_left = MINUTES_IN_A_HALF_HOUR - transpired_access_minutes
            # print(f'7722 access_minutes_left:{access_minutes_left}')



            access_token_issue_date = datetime.now(timezone.utc).isoformat()

            # print(f'7760 new access_token_issue_date type:{type(access_token_issue_date)}, value:{access_token_issue_date}')













            # publish the new acces token as soon as we have it
            # if gbl_mqtt_client != None:

            #     payload = json.dumps({
            #         "refreshToken": refresh_token,
            #         "accessToken": access_token,
            #         "acctHash": hashVal
            #     })
            #     mqtt_publish(gbl_mqtt_client, CREDS_INFO_TOPIC, payload)

            publish_creds_account_data()



            # Construct the JSON object
            token_data = {
                "access_token_issued": now_iso,
                "refresh_token_issued": refresh_token_issue_date,
                "token_dictionary": {
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                    "refresh_token": new_refresh_token, 
                    "access_token": new_access_token,  
                    "id_token": new_id_token   
                }
            }


            # print(f'2 saving token object (with updated access token) to file {mri_tokens_file}')
            print(f'2804 Persisting token object (with updated access token)')

            # Save the JSON object to the file
            with open(mri_tokens_file, "w") as f:
                json.dump(token_data, f, indent=4)

            # print(f"2 Token data saved to {mri_tokens_file}")


        except Exception as e:
            print(f"4230 refresh_auth_token() error: {e}")





    try:

        get_user_preferences(access_token)

    except Exception as e:
        print(f"3722 Error getting get_user_preferences: {e}")
        raise


    try:

        # Calculate minutes transpired since token issuance
        # now = datetime.datetime.now(datetime.timezone.utc)
        now = datetime.now(timezone.utc)

    except Exception as e:
        print(f"120 Error getting now utc: {e}")

    try:
        if access_token_issue_date:
            # access_dt = datetime.datetime.fromisoformat(access_token_issue_date)
            access_dt = datetime.fromisoformat(access_token_issue_date)
            transpired_access_minutes = (int)((now - access_dt).total_seconds() / 60)
            access_minutes_left = MINUTES_IN_A_HALF_HOUR - transpired_access_minutes
            # print(f'7726 access_minutes_left:{access_minutes_left}')

    
        
        if refresh_token_issue_date:
            # refresh_dt = datetime.datetime.fromisoformat(refresh_token_issue_date)
            refresh_dt = datetime.fromisoformat(refresh_token_issue_date)
            transpired_refresh_minutes = (int)((now - refresh_dt).total_seconds() / 60)
            refresh_minutes_left = MINUTES_IN_A_WEEK - transpired_refresh_minutes 
            refresh_days_left = refresh_minutes_left/1440

    except Exception as e:
        print(f"130 Error getting issue datetime: {e}")


    current_time = datetime.now()
    current_time_str = current_time.strftime('%H:%M:%S')

    print(f'\n>>>>>>>> manage_tokens.py <<<<<<<<')
    print(f'Token data loaded at {current_time_str}')
    print(f"Transpired access minutes: {transpired_access_minutes}")
    print(f"access_minutes_left: {access_minutes_left}")
    print(f"Transpired refresh minutes: {transpired_refresh_minutes}")
    print(f"refresh_minutes_left: {refresh_minutes_left} ({refresh_days_left:.2f} days)")
    # print(f"current local time: {current_time_str}")





def get_current_utc():

    utc_now = datetime.now(timezone.utc)
    print(f"Current UTC Time: {utc_now}")
    pass

    return utc_now





def refresh_auth_token():
    global access_token, refresh_token, old_access_token, old_refresh_token
    global new_access_token_flag

    current_time = datetime.now()
    current_time_str = current_time.strftime('%H:%M:%S')

    print(f'"*** REFRESHING ACCESS TOKEN ***  at {current_time_str}')

    # Base64 encode client_id:client_secret
    # credentials = f"{client_id}:{client_secret}"
    credentials = f"{appKey}:{appSecret}"
    base64_credentials = base64.b64encode(credentials.encode()).decode()

    try:
        # # Read previous tokens from files
        # with open(f"{SPX_HOME}/.access", "r") as f:
        #     old_access_token = f.read().strip()

        old_access_token = access_token

        # with open(f"{SPX_HOME}/.refresh", "r") as f:
        #     old_refresh_token = f.read().strip()

        old_refresh_token = refresh_token

        # Prepare request payload
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {base64_credentials}"
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": old_refresh_token
        }

        # Send POST request to Schwab API
        response = requests.post("https://api.schwabapi.com/v1/oauth/token", headers=headers, data=data)

        if response.status_code == 200:
            new_access_token_flag = True

            print("... got a new access token")
            # print(f'\nresponse:\n{response.json()}\n')
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}, returning")
            raise


        # Generate formatted timestamp
        formatted_date = datetime.now(timezone.utc).isoformat()

        # Construct token dictionary
        mqtt = {
            "access_token_issued": formatted_date,
            "refresh_token_issued": formatted_date,
            "token_dictionary": response.json()
        }

        print(f'mqtt:{mqtt}')

        # Extract new tokens
        access_token = mqtt["token_dictionary"].get("access_token")
        refresh_token = mqtt["token_dictionary"].get("refresh_token")

        # # Save tokens to files
        # try:
        #     with open(f"{SPX_HOME}/.access", "w") as f:
        #         f.write(access_token)
        # except Exception as err:
        #     print("Error saving Access Token:", err)

        # try:
        #     with open(f"{SPX_HOME}/.refresh", "w") as f:
        #         f.write(refresh_token)
        # except Exception as err:
        #     print("Error saving Refresh Token:", err)

        # try:
        #     with open(f"{SPX_HOME}/.tokens.json", "w") as f:
        #         json.dump(mqtt, f, indent=2)
        # except Exception as err:
        #     print("Error saving tokens.json:", err)

        # return mqtt["token_dictionary"]

        return response.json()

    except Exception as error:
        print(f"Error refreshing auth token: {error}")
        raise









def get_tokens():

    global appKey
    global appSecret
    global access_token 
    global refresh_token 
    global id_token
    global acctNum
    global hashVal
    global arg_one
    global gbl_mqtt_client 



    refresh_expired_flag = False









    appKey, appSecret, tokensFile = load_env_variables()


    try:

        refresh_minutes_left = 0




        token_file_data = read_tokens_mri_file()
        # print(f'2930 got token_file_data')
        # print(f'token_file_data type:{type(token_file_data)}, data:\n{token_file_data}')


        print(f'calling refresh_token_minutes_left()')
        refresh_minutes_left = refresh_token_minutes_left(token_file_data)
        print(f'my_refresh_minutes_left:{refresh_minutes_left}')

        if refresh_minutes_left < 1:
            refresh_expired_flag = True



        if not refresh_expired_flag:

            extract_tokens_mri(token_file_data)
            # print(f'2932 extracted token_file_data')
            # print(f'9028 refersh_token:{refresh_token}\naccess_token:{access_token}')


    except Exception as e:
        print(f"496 read_tokens_mri_file error: {e}")
        raise






    try:

        if not refresh_expired_flag:

            account_file_data = read_acct_mri_file()
            # print(f'2940 got account_file_data')



            # print(f'8920 account_file_data  type:{type(account_file_data )}, data:\n{account_file_data }')
            extract_account_mri(account_file_data)
            # print(f'2942 extracted account_file_data')
            # print(f'9029 hashVal:{hashVal}\nacctNum:{acctNum}')


    except Exception as e:
        print(f"4929 read_acct_mri_file: {e}")
        arg_one = "refresh"
        


    

    # acctNum = None # simulates bad file data

    all_initialized_flag = all_tokens_initialized()
    if all_initialized_flag == True:
        print(f'all tokens have been initialized')
    else:
        print(f'NOT all tokens have been initialized')







    # print()
    # print(f'access_token type:{type(access_token)}, value:<{access_token}>')
    # print(f'refresh_token type:{type(refresh_token)}, value:<{refresh_token}>')    
    # print(f'id_token type:{type(id_token)}, value:<{id_token}>') 

    if refresh_minutes_left == None:
        refresh_days_left = 0

    else:
        refresh_days_left = refresh_minutes_left/MINUTES_IN_A_DAY



    if refresh_days_left < 1:
        print(f'!!!!!!!!! WARNING !!!!!!!!!! refresh token expires in {refresh_days_left:.1f} days')



    
    # if arg_one == "refresh" or all_initialized_flag == False or refresh_expired_flag == True:
    if arg_one == "refresh" or all_initialized_flag == False:


        if arg_one == "refresh":
            print(f'command line refresh requested')
            

        else:
            print(f'Less than a day left in the refresh token. Days left:{refresh_days_left}. Renewing')

        arg_one = None
    

        authUrl = f'https://api.schwabapi.com/v1/oauth/authorize?client_id={appKey}&redirect_uri=https://127.0.0.1'



        # Open the URL in the default web browser
        webbrowser.open(authUrl)
        print(f"Opening browser for authentication: {authUrl}")


        # print(f"Click to authenticate: {authUrl}")

        returnedLink = input("Paste the redirect URL here:")

        code = f"{returnedLink[returnedLink.index('code=')+5:returnedLink.index('%40')]}@"

        print(f'code:{code}')


        headers = {'Authorization': f'Basic {base64.b64encode(bytes(f"{appKey}:{appSecret}", "utf-8")).decode("utf-8")}', 'Content-Type': 'application/x-www-form-urlencoded'}
        data= {'grant_type': 'authorization_code', 'code': code, 'redirect_uri': 'https://127.0.0.1'}
        response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)

        print(f'oauth response.status_code:{response.status_code}')

        responseCode = response.status_code
        
        # responseCode = 400 # test failed post
        if responseCode != 200:
            print(f'\n\n!!!!!!!!!!!!!! oauth failed! response.status_code:{responseCode}, text:{response.text}')
            print(f'deleting {mri_tokens_file} and exiting the program!!!!')


            # Ensure the file exists before attempting to delete
            if os.path.exists(mri_tokens_file):
                os.remove(mri_tokens_file)
                print(f"Deleted: {mri_tokens_file}")
            else:
                print(f"File not found: {mri_tokens_file}")
                pass
            
            os._exit(1)  # Immediately terminates the entire process

        else:
            print(f'oauth succeeded')





        tD = response.json()

        # print(f'tD type:{type(tD)}, data:\n{tD}')

        access_token = tD['access_token']
        refresh_token = tD['refresh_token']
        id_token = tD['id_token']

        # print()
        # print(f'access_token type:{type(access_token)}, value:<{access_token}>')
        # print(f'refresh_token type:{type(refresh_token)}, value:<{refresh_token}>')    
        # print(f'id_token type:{type(id_token)}, value:<{id_token}>') 



        base_url = 'https://api.schwabapi.com/trader/v1'

        full_url = f'{base_url}/accounts/accountNumbers'

        response = requests.get(f'{base_url}/accounts/accountNumbers', headers={'Authorization': f'Bearer {access_token}'})
        
        # print(f'7021 accountNumbers:{response.status_code}')
        # print(f'7022 data:{response.json()}')
        
        # print(response.json())

        
        # Save JSON response
        response_json = response.json()
        # print(f'7934 response_json:{response_json}')
        # print(f'7935 full_url:{full_url}')

        # Extract values
        acctNum = response_json[0]['accountNumber']
        hashVal = response_json[0]['hashValue']

        print()
        print(f'acctNum type:{type(acctNum)}, value:<{acctNum}>')
        print(f'hashVal type:{type(hashVal)}, value:<{hashVal}>')   

        utc_val = get_current_utc()
        # print(f'0792 utc_val type:{type(utc_val)}, value:{utc_val}')

        

        # Convert datetime object to ISO format string
        utc_json = json.dumps({"utc_val": utc_val.isoformat()})

        # print(f'0794 utc_json type: {type(utc_json)}, value: {utc_json}')



            
        # Generate timestamps for access and refresh token issuance
        now_iso = datetime.now(timezone.utc).isoformat()



        # Construct the JSON object
        token_data = {
            "access_token_issued": now_iso,
            "refresh_token_issued": now_iso,
            "token_dictionary": {
                "expires_in": 1800,
                "token_type": "Bearer",
                "scope": "api",
                "refresh_token": refresh_token,  # Previously stored global variable
                "access_token": access_token,    # Previously stored global variable
                "id_token": id_token             # Previously stored global variable
            }
        }

        # print(f'834-1 saving token object to file {mri_tokens_file}')

        # Save the JSON object to the file
        with open(mri_tokens_file, "w") as f:
            json.dump(token_data, f, indent=4)

        print(f"834-1 Token data saved to {mri_tokens_file}")



        # Construct the JSON object
        acct_data = {
            "account_number": acctNum,
            "account_hash": hashVal
        }

        # print(f'834-2 saving account object to file {mri_acct_file}')

        # Save the JSON object to the file
        with open(mri_acct_file, "w") as f:
            json.dump(acct_data, f, indent=4)

        print(f"834-3 Account data saved to {mri_acct_file}")
















    # else check to see if access token needs to be refreshed
    else:



        if access_minutes_left <= 5:
        # if access_minutes_left <= 20:

            try:
                token_dict = refresh_auth_token()
                # print(f'token_dict type{type(token_dict)}, data:\n{token_dict}')

                now_iso = datetime.now(timezone.utc).isoformat()

                # Extract token values from dictionary
                new_refresh_token = token_dict.get("refresh_token")
                new_access_token = token_dict.get("access_token")
                new_id_token = token_dict.get("id_token")

                # Display extracted values
                # print(f"New Refresh Token: {new_refresh_token}")
                print(f"8302\n  old access token:{access_token}\n  New Access Token: {new_access_token}")
                # print(f"New ID Token: {new_id_token}")

                access_token = new_access_token


                # publish the new acces token as soon as we have it
                # if gbl_mqtt_client != None:

                #     payload = json.dumps({
                #         "refreshToken": refresh_token,
                #         "accessToken": access_token,
                #         "acctHash": hashVal
                #     })
                #     mqtt_publish(gbl_mqtt_client, CREDS_INFO_TOPIC, payload)

                publish_creds_account_data()



                # Construct the JSON object
                token_data = {
                    "access_token_issued": now_iso,
                    "refresh_token_issued": refresh_token_issue_date,
                    "token_dictionary": {
                        "expires_in": 1800,
                        "token_type": "Bearer",
                        "scope": "api",
                        "refresh_token": new_refresh_token, 
                        "access_token": new_access_token,  
                        "id_token": new_id_token   
                    }
                }


                # print(f'2 saving token object (with updated access token) to file {mri_tokens_file}')
                print(f'Persisting token object (with updated access token)')

                # Save the JSON object to the file
                with open(mri_tokens_file, "w") as f:
                    json.dump(token_data, f, indent=4)

                # print(f"2 Token data saved to {mri_tokens_file}")


            except Exception as e:
                print(f"1230 refresh_auth_token() error: {e}")

    



# MQTT Configuration
BROKER_ADDRESS = "localhost"
PORT_NUMBER = 1883
CREDS_REQUEST_TOPIC = "mri/creds/request/#"
CREDS_INFO_TOPIC = "mri/creds/info"
CREDS_REQUEST_PREFIX = "mri/creds/request"

# MQTT Client Setup
def on_connect(client, userdata, flags, rc):
    print("\n+++++++++++++++\nConnected to MQTT Broker with result code", rc)
    client.subscribe(CREDS_REQUEST_TOPIC)
    print(f"Subscribed to topic: {CREDS_REQUEST_TOPIC}")


def publish_creds_account_data():

    # print(f'pcad access_token:{access_token}')

    payload = json.dumps({
        "refreshToken": refresh_token,
        "accessToken": access_token,
        "acctHash": hashVal,
        "streamerUrl": streamer_socket_url,
        "customerId": customer_id,
        "correlId": correl_id,
        "channel": channel,
        "functionId": function_id

    })
    mqtt_publish(gbl_mqtt_client, CREDS_INFO_TOPIC, payload)


def on_message(client, userdata, msg):
    print(f"\n^^^^^^^^^^\nReceived MQTT message topic:{msg.topic} payload:{msg.payload.decode('utf-8')}")

    if CREDS_REQUEST_PREFIX in msg.topic:
        print(f'received CREDS request')
        publish_creds_account_data()




mqtt_publish_lock = threading.Lock()

def mqtt_publish(client, topic, payload):

    with mqtt_publish_lock:
        client.publish(topic, payload)
    # print(f"Published to {topic}: {payload}")

# def mqtt_services():
#     client = mqtt.Client()
#     client.on_connect = on_connect
#     client.on_message = on_message

#     client.connect(BROKER_ADDRESS, PORT_NUMBER, 60)
#     client.loop_forever()


publish_credential_lock = threading.Lock()

def publish_credentials_thread(client):

    global new_access_token_flag


    time.sleep(10)

    pub_creds_loop_cnt = 0
    new_access_token_broadcast_count = 0

    while True:
        pub_creds_loop_cnt += 1

        if new_access_token_flag:
            # print(f'3956 pub creds thread: new access token')
            new_access_token_flag = False
            new_access_token_broadcast_count = 6

        if new_access_token_broadcast_count > 0:
            # print(f'3957 pub creds publishing cnt:{new_access_token_broadcast_count}')
            new_access_token_broadcast_count -= 1
            publish_creds_account_data()


        else:

            if pub_creds_loop_cnt % 10 == 2:
                publish_creds_account_data()

        time.sleep(1)


def mqtt_services():
    global gbl_mqtt_client 

    client = mqtt.Client()
    gbl_mqtt_client  = client
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_ADDRESS, PORT_NUMBER, 60)

    # Start a background thread to publish credentials every 10 seconds
    credentials_thread = threading.Thread(target=publish_credentials_thread, args=(client,), daemon=True)
    credentials_thread.start()

    client.loop_forever()





def manage_tokens_task():
    while True:
        try:
            # my_time = datetime.now()
            # my_time_str = my_time.strftime('%H:%M:%S') 
            # print(f'\n\n3044 calling get_tokens at {my_time_str}')

            # print(f'mt 100010')

            get_tokens()

            # print(f'mt 100020')

        except Exception as e:
            my_time = datetime.now()
            # my_time_str = time.strftime('%H:%M:%S') + f".{time.microsecond // 1000:03d}" 
            my_time_str = my_time.strftime('%H:%M:%S') 
            if "timed out" in str(e):
                print(f'get_tokens() returned timed out error at {my_time_str}, e:{e}')
            else:
                print(f'get_tokens() returned an error at {my_time_str}, e:{e}')

            time.sleep(5)
            continue


        # print(f'mt 100030')


        market_open_flag, current_eastern_time, seconds_to_next_minute = market_open.is_market_open2(open_offset=0, close_offset=0)

        seconds_to_next_minute += 40

        # time.sleep(60)
        time.sleep(seconds_to_next_minute)

        # print(f'mt 100040')

def main():
    global arg_one

    









    # Check for command-line argument
    arg_one = sys.argv[1] if len(sys.argv) > 1 else None

    if arg_one:
        print(f"Command-line argument provided, arg_one type:{type(arg_one)}, value:{arg_one}")
    else:
        print("No command-line argument provided.")

    # Create threads
    tokens_thread = threading.Thread(target=manage_tokens_task, daemon=True)
    mqtt_thread = threading.Thread(target=mqtt_services, daemon=True)

    # Start threads
    tokens_thread.start()
    mqtt_thread.start()

    # Keep main thread alive
    while True:
        time.sleep(1)

# Entry point of the program
if __name__ == "__main__":
    print("manage_tokens: startup\n")
    main()
    print("\nmanage_tokens: exiting\n")






