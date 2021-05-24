#!/usr/bin/env python3

import urllib3
import base64
import json
import datetime
import os.path
import csv


class BATCH_FILE:
    def __init__(self):
        self.path = '/home/stephen/Sync/Work/Projects/EBBP/'

    def read_from_csv(self, file_name):
        batchRecords = []

        with open(self.path + file_name) as f:
            reader = csv.DictReader(f)
            for row in reader:
                batchRecords.append(row)

        return(batchRecords)

    def print_json(self, jsonArray):
        jsonString = json.dumps(jsonArray, indent=4)
        print(jsonString)


class EBBP_API:
    def __init__(self):
        self.https = urllib3.PoolManager()
        self.hostname = 'https://api.universalservice.org'
        # self.hostname = 'https://api-stg.universalservice.org'
        self.usac_url = self.hostname + '/ebbp-svc/1'
        self.base_path = '/home/stephen/Sync/Work/Projects/EBBP/'
        if self.hostname.find('api-stg') > 0:
            self.token_filename = 'ebbp-tokens-stg'
        else:
            self.token_filename = 'ebbp-tokens'
        self.access_token = ''
        self.token_expiration = None
        self.response_status = 0
        self.response_headers = {}
        self.response_data = {}

    def get_tokens(self):

        if not os.path.isfile(self.token_filename):
            print('Token file does not exist: %s' % self.token_filename)
            return(False)

        # Use file modification time as timestamp for token
        mtime = os.path.getmtime(self.token_filename)
        token_created = datetime.datetime.fromtimestamp(mtime)

        # Read json formatted tokens as a single line
        with open(self.token_filename) as f:
            line = f.read()

        if len(line) == 0:
            print('Token file %s is empty.' % self.token_filename)
            return(False)
        else:
            json_record = json.loads(line)
            self.access_token = json_record['access_token']
            token_duration = json_record['expires_in']
            # Adjust expiration by 1 minute for good measure
            self.token_expiration = token_created + \
                datetime.timedelta(seconds=token_duration-60)
            # Check if token has expired
            if datetime.datetime.now() > self.token_expiration:
                return(False)

        return(True)

    def write_tokens(self):

        with open(self.token_filename, "w") as f:
            record = json.dumps(self.response_data)
            f.write(record)

        return(True)

    def connect_basic(self, username, password):

        # Tested and working

        # Base64 encode the API credentials
        auth_b64 = base64.b64encode(bytes(username + ':' + password, 'utf-8'))
        credentials = auth_b64.decode(encoding="utf-8", errors="strict")

        print('Sending auth... ', end='')
        # Send a Basic Authentication request
        auth_request = self.https.request(
            'POST',
            'https://api-stg.universalservice.org/auth/token',
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Basic ' + credentials
            }
        )

        # Parse the connect data and check for success
        self.response_status = auth_request.status
        self.response_data = json.loads(auth_request.data)
        self.response_headers = auth_request.headers
        print(self.response_status, end='')

        if (self.response_status == 200):
            if (self.response_data['token_type'] == 'Bearer'):
                token_duration = self.response_data['expires_in']
                self.token_expiration = datetime.datetime.now() + \
                    datetime.timedelta(seconds=token_duration)
                self.access_token = self.response_data['access_token']
                print(' succeeds.')
                connect_status = True
        else:
            print(' fails.')
            connect_status = False

        return(connect_status)

    def batch_subscriber_upload(self, csvfile):

        # Not working: Error 400: File is invalid

        # Open the CSV file to send
        file_name = self.base_path + csvfile
        with open(file_name) as fp:
            file_data = fp.read()
        # print('Data=%s' % file_data)

        request_url = self.usac_url + '/batch'
        print('URL=%s' % request_url)
        print('Sending CSV file... ', end='')

        # Send a request for transaction report
        request = self.https.request(
            'POST',
            request_url,
            headers={
                'Content-Type': 'multipart/form-data',
                'Authorization': 'Bearer ' + self.access_token
            },
            fields={
                'filefield': (csvfile, file_data)
            }
        )

        # Parse the data and extract the report
        self.response_status = request.status
        print(self.response_status)
        self.response_data = json.loads(request.data.decode('utf-8'))
        print(self.response_data)

        if (self.response_status == 200):
            print('CSV send succeeds.')
            csv_status = True
        else:
            print('CSV send failed.')
            csv_status = False

        return(csv_status)

    def get_batch_status(self):

        # Tested and working

        request_url = self.usac_url + '/batch'
        # print(request_url)
        print('Getting batch status... ', end='')

        # Send a request for transaction report
        request = self.https.request(
            'GET',
            request_url,
            headers={
                'Content-Type': 'text/plain',
                'Authorization': 'Bearer ' + self.access_token
            }
        )

        # Parse the response data
        self.response_status = request.status
        print(self.response_status, end='')
        self.response_data = json.loads(request.data.decode('utf-8'))

        if (self.response_status == 200):
            print(' succeeds.')
            self.write_batch_status()
            batch_status = True
        else:
            print(' fails.')
            self.write_api_response()
            batch_status = False

        return(batch_status)

    def write_batch_status(self):

        if len(self.response_data) == 0:
            return(False)

        d = datetime.datetime.now()
        csv_file = self.base_path + \
            d.strftime('batch_status_%Y%m%d%H%M.csv')

        # Extract CSV column headers from dictionary
        csv_columns = []
        for key in self.response_data[0].keys():
            csv_columns.append(key)

        # Write CSV data rows
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for i in range(len(self.response_data)):
                writer.writerow(self.response_data[i])
            print("Wrote batch status file: %s" % csv_file)

        return(True)

    def enroll_subscriber(self, batchRecord, verifyOnly=False):

        # Tested and working
        if verifyOnly:
            print('Verifying subscriber... ', end='')
            request_url = self.usac_url + '/verify'
        else:
            print('Enrolling subscriber... ', end='')
            request_url = self.usac_url + '/subscriber'

        # Fix SSN issue
        if batchRecord['bqpLast4ssn'] == '0':
            batchRecord['bqpLast4ssn'] = ''

        encoded_data = json.dumps(batchRecord).encode('utf-8')
        # Send a request for transaction report
        request = self.https.request(
            'POST',
            request_url,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + self.access_token
            },
            body=encoded_data
        )

        # Parse the data and extract the report
        self.response_status = request.status
        print(self.response_status, end='')
        self.response_data = json.loads(request.data.decode('utf-8'))

        if (self.response_status == 200):
            print(' succeeds.')
            self.write_api_response()
            method_status = True
        if (self.response_status == 201):
            print(' succeeds.')
            self.write_api_response()
            method_status = True
        elif(self.response_status == 400):
            print(' fails.')
            self.write_api_response()
            method_status = False
        else:
            method_status = False

        return(method_status)

    def de_enroll_subscriber(self, batchRecord):

        print('De-enrolling subscriber... ', end='')
        request_url = self.usac_url + '/subscriber'
        print(request_url)

        encoded_data = json.dumps(batchRecord).encode('utf-8')
        # Send a request for transaction report
        request = self.https.request(
            'DELETE',
            request_url,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + self.access_token
            },
            body=encoded_data
        )

        # Parse the data and extract the report
        self.response_status = request.status
        print(self.response_status)

        if (self.response_status == 200):
            print('De-enroll succeeds.')
            self.response_data = json.loads(request.data.decode('utf-8'))
            print(self.response_data)
            de_enroll_status = True
        elif(self.response_status == 400):
            print('De-enroll failed.')
            self.response_data = json.loads(request.data.decode('utf-8'))
            print(self.response_data)
            de_enroll_status = False
        else:
            de_enroll_status = False

        return(de_enroll_status)

    def update_subscriber(self, batchRecord):

        print('Updating subscriber... ', end='')
        request_url = self.usac_url + '/subscriber'
        print(request_url)

        encoded_data = json.dumps(batchRecord).encode('utf-8')
        # Send a request for transaction report
        request = self.https.request(
            'PUT',
            request_url,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + self.access_token
            },
            body=encoded_data
        )

        # Parse the data and extract the report
        self.response_status = request.status
        print(self.response_status)

        if (self.response_status == 200):
            print('Update succeeds.')
            self.response_data = json.loads(request.data.decode('utf-8'))
            print(self.response_data)
            update_status = True
        elif(self.response_status == 400):
            print('Update failed.')
            self.response_data = json.loads(request.data.decode('utf-8'))
            print(self.response_data)
            update_status = False
        else:
            update_status = False

        return(update_status)

    def get_subscriber_report(self, reportType, sac, includeID):

        # Tested and working
        print('Getting subscriber report... ', end='')

        # Send a request for subscriber report
        request = self.https.request(
            'GET',
            self.usac_url + '/report/subscriber',
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + self.access_token
            },
            fields={
                'reportType': reportType,
                'sac': sac,
                'includeSubscriberId': includeID
            }
        )

        # Parse the response data
        self.response_status = request.status
        print(self.response_status, end='')
        self.response_data = request.data.decode('utf-8')

        if (self.response_status == 200):
            print(' succeeds.')
            self.write_report('subscriber')
            report_status = True
        else:
            print(' fails.')
            # print(self.response_data)
            self.write_api_response()
            report_status = False

        return(report_status)

    def get_transaction_report(self, reportType, sac, startDate,
                               endDate, type, includeSubscriberID):

        # Tested and working
        print('Getting transaction report... ', end='')

        # Send a request for transaction report
        request = self.https.request(
            'GET',
            self.usac_url + '/report/transaction',
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + self.access_token
            },
            fields={
                'reportType': reportType,
                'sac': sac,
                'startDate': startDate,
                'endDate': endDate,
                'type': type,
                'includeSubscriberId': includeSubscriberID
            }
        )

        # Parse the response data
        self.response_status = request.status
        print(self.response_status, end='')
        self.response_data = request.data.decode('utf-8')

        if (self.response_status == 200):
            print(' succeeds.')
            self.write_report('transaction')
            transaction_status = True
        else:
            print(' fails.')
            self.write_api_response()
            transaction_status = False

        return(transaction_status)

    def write_report(self, reportType):

        d = datetime.datetime.now()
        file_name = self.base_path + \
            d.strftime(reportType + '_%Y%m%d%H%M.csv')

        with open(file_name, "w") as f:
            f.write(self.response_data)
            print("Wrote report file: %s" % file_name)

        return(True)

    def write_api_response(self):

        d = datetime.datetime.now()

        file_name = self.base_path + \
            d.strftime('api_response_%Y%m%d%H%M.json')

        with open(file_name, "a") as f:
            f.write(json.dumps(self.response_data, indent=4))
            print("Wrote report file: %s" % file_name)

        return(True)


if __name__ == '__main__':
    e = EBBP_API()

    # Authenticate to the EBBP server
    if e.get_tokens() is False:
        print('No valid access tokens in %s' % e.token_filename)
        e.connect_basic('stephen@pinebelt.net', 'S$w73Tsf4#')
        e.write_tokens()
    else:
        print('Found valid access tokens in %s' % e.token_filename)

    # Batch status unit test
    # e.get_batch_status()

    # Subscriber/Transaction Report unit tests
    # e.get_subscriber_report('detail', '825010', '1')
    # e.get_transaction_report('detail', '825010', '05/01/2021',
    #                          '05/20/2021', 'enroll', '1')

    # Enroll / Verify unit test
    b = BATCH_FILE()
    batchRecords = b.read_from_csv('852009_05_21_2021_12_05_15.csv')
    for i in range(len(batchRecords)):
        e.enroll_subscriber(batchRecords[i], verifyOnly=False)
    exit()
