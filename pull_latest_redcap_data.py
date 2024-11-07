# pull_latest_redcap_data.py
#
# writes two files to the destination directory
# one with the date appended, and one with just the output file name.
import sys
import os
import requests
import os.path
import datetime

# configuration
DESTINATION_DIRECTORY="~/data/redcap"
DESTINATION_BASE_FILENAME='Redcap_data_report'

RECAP_REPORT_NUMBERS = [
    270041,
    247884, 
    242544, 
    251954,
    259920,
    307916, 
    307918, 
    307920, 
    307922
]

# define request data
REPORT_REQUEST_DATA = {
    'token': os.environ['REDCAP_TOK'],
    'content': 'report',
    'format': 'csv',
    'report_id': '', # '270041',
    'csvDelimiter': '',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'returnFormat': 'json'
}

def write_result_data_to_filename(filename, txt):
    sys.stderr.write(f'Writing Redcap data to {filename}...')
    sys.stderr.flush()
    with open(filename, 'wt') as f:
        f.write(txt)
    sys.stderr.write('OK.\n')
    sys.stderr.flush()


def format_report_destination_filename(report_number, now_flag):
    if now_flag:
        filename = os.path.join(DESTINATION_DIRECTORY, 
                                DESTINATION_BASE_FILENAME + '_' + str(report_number) + '.' + REPORT_REQUEST_DATA['format'])
    else:
        filename = os.path.join(DESTINATION_DIRECTORY, 
                                DESTINATION_BASE_FILENAME + '_' + str(report_number) + '_' + datetime.datetime.now().strftime("%d-%b-%Y_%H-%M-%S") + '.' + REPORT_REQUEST_DATA['format'])
   
    filename = os.path.expanduser(filename)
    return filename 


def pull_recap_report_data(report_number):
    REPORT_REQUEST_DATA['report_id'] = str(report_number)
    sys.stderr.write(f'Requesting Redcap Report #{REPORT_REQUEST_DATA["report_id"]}...')
    sys.stderr.flush()
    r = requests.post('https://redcap.iths.org/api/',data=REPORT_REQUEST_DATA)
    sys.stderr.write('request complete...OK.\n')
    sys.stderr.flush()
    sys.stderr.write(f'HTTP Status: {r.status_code}\n')
    sys.stderr.flush()
    if r.status_code == 200:
        # write file to base report output location
        filename = format_report_destination_filename(report_number, False)
        write_result_data_to_filename(filename, r.text)
        # write file to dated report output location
        filename = format_report_destination_filename(report_number, True)
        write_result_data_to_filename(filename, r.text)
    else:
        sys.stderr.write(f'HTTP Status != 200, not writing data files for report id {report_number}.\n')

# run as main script
if __name__ == '__main__':
    # loop over report numbers and pull each in turn
    for report_number in RECAP_REPORT_NUMBERS:
        pull_recap_report_data(report_number)




