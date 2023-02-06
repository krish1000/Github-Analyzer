

import sys
import socket
import random
import time
# new:
import requests #had to pip install
import os
import re

# data_to_send = "" #contains data per 15 seconds

# Helper method to print
def print_data(json_data, progLanguage):
    data_to_send = ""
    try:
        for item in json_data['items']:
            dateformatted = (item['pushed_at'][:-1]).split("T")
            descriptionRe = re.sub('[^a-zA-Z ]', '', str(item['description'] or ''))
            data_to_send += progLanguage + "\t:" + item['full_name'] + "\t:" + dateformatted[0] + " " + dateformatted[1] + "\t:" + str(item['stargazers_count']) + "\t:" + descriptionRe + "\n"
            print(progLanguage + "\t:" + item['full_name'] + "\t:" + dateformatted[0] + " " + dateformatted[1] + "\t:" + str(item['stargazers_count']) + "\t:" + descriptionRe)

            # break ############# MAKE SURE TO REMOVEEE, TESTING FOR 1 CURENTLY
    except:
        pass
        # print("ERROR")
        # print(json_data)
        # exit()
        #     s.shutdown(socket.SHUT_RD)
    return data_to_send
        

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")

try:

    # NOTE:
    # The below code before the while loop is used to request data from github's api
    # Requests repo data that have CSharp, Java, Python as their main code langauge

    token = "token " + os.getenv('TOKEN')
    # print(token + "")
    # # token = "token " + "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" #TEMP, when running spark make sure to switcharoo to above line

    # csharp_url = 'https://api.github.com/search/repositories?q=+language:CSharp&sort=updated&order=desc&per_page=50'
    # java_url = 'https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50'
    # python_url = 'https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50'

    # csharp_json = requests.get(csharp_url, headers={"Authorization": token}).json()
    # java_json = requests.get(java_url, headers={"Authorization": token}).json()
    # python_json =  requests.get(python_url, headers={"Authorization": token}).json()

    # ##### INITIAL DELAY OF 15 SECONDS
    # ###################### CHANGEEEE TO 15 AGAINNNNNNNNNNNNNN
    # time.sleep(15) #use 15, temporarily lower for testing

    csharp_url = 'https://api.github.com/search/repositories?q=+language:CSharp&sort=updated&order=desc&per_page=50'
    java_url = 'https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50'
    python_url = 'https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50'

    while True:
        csharp_json = requests.get(csharp_url, headers={"Authorization": token}).json()
        java_json = requests.get(java_url, headers={"Authorization": token}).json()
        python_json =  requests.get(python_url, headers={"Authorization": token}).json()

        data = ""
        # data += "CSHARP" + "\n"
        # print("CSHARP") # Used as an identifier to spark; to show next lines are CSharp repos
        data += print_data(csharp_json, "CSHARP")

        # data += "JAVA" + "\n"
        # print("JAVA") # Used as an identifier to spark; to show next lines are Java repos
        data += print_data(java_json, "JAVA")

        # data += "PYTHON" + "\n"
        # print("PYTHON") # Used as an identifier to spark; to show next lines are Python repos
        data += print_data(python_json, "PYTHON")

        # print(data)
        # original:
        # number = random.randint(1, 1000000000)
        # data = f"{number}\n".encode()
        # conn.send(data)
        # print(number)

        conn.send(data.encode())

        # NOTE I intentionally placed request pre-whileloop and inside loop
        # Reason being the data can be fetched prior and will always be sent at exactly 15 seconds
        # If I were to time.sleep(15) before fetching the next data it will not be as accurate due to traffic delays/jitter
        # So best practise would be to fetch data prior to sending to spark, and then print the previous data on next iteration

        # reset data to send; in preperation of new data:
        # data = ""

        # token = "token " + os.getenv('TOKEN')
        # token = "token " + "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" #TEMP, when running spark make sure to switcharoo to above line

        # csharp_url = 'https://api.github.com/search/repositories?q=+language:CSharp&sort=updated&order=desc&per_page=50'
        # java_url = 'https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50'
        # python_url = 'https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50'

        # csharp_json = requests.get(csharp_url, headers={"Authorization": token}).json()
        # java_json = requests.get(java_url, headers={"Authorization": token}).json()
        # python_json =  requests.get(python_url, headers={"Authorization": token}).json()

        ###################### CHANGEEEE TO 15 AGAINNNNNNNNNNNNNN
        time.sleep(15) #use 15, temporarily lower for testing
except KeyboardInterrupt:
    s.shutdown(socket.SHUT_RD)