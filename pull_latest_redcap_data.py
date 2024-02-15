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
DESTINATION_DIRECTORY="/home/azureuser/data/redcap"
DESTINATION_BASE_FILENAME='Redcap_data_report_270041'

# define request data
data = {
    'token': os.environ['REDCAP_TOK'],
    'content': 'report',
    'format': 'csv',
    'report_id': '270041',
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

# run as main script
if __name__ == '__main__':
    sys.stderr.write(f'Requesting Redcap Report #{data["report_id"]}...')
    sys.stderr.flush()
    r = requests.post('https://redcap.iths.org/api/',data=data)
    sys.stderr.write('request complete...OK.\n')
    sys.stderr.flush()
    sys.stderr.write(f'HTTP Status: {r.status_code}\n')
    sys.stderr.flush()
    if r.status_code == 200:
        # write file to base report output location
        filename = os.path.join(DESTINATION_DIRECTORY, DESTINATION_BASE_FILENAME + '.' + data['format'])
        write_result_data_to_filename(filename, r.text)
        # write file to dated report output location
        filename = os.path.join(DESTINATION_DIRECTORY, DESTINATION_BASE_FILENAME + datetime.datetime.now().strftime("%d-%b-%Y_%H-%M-%S") + '.' + data['format'])
        write_result_data_to_filename(filename, r.text)
    else:
        sys.stderr.write('HTTP Status != 200, not writing data files.\n')



