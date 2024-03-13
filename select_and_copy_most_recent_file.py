# select_and_copy_most_recent_file.py
# identify by date pattern and copy
# most recent file to a given file name
import sys
import argparse
import glob
import shutil
import re
import datetime

PATTERNS = [
	r'.*-(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\Z',
	r'.*-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})\Z',
]

def convert_to_datetime(year_str, month_str, day_str):
	return datetime.datetime(year=int(year_str), month=int(month_str), day=int(day_str))

def extract_datetime_from_filename(filename):
	# look one of these patterns:
	# stuff-YYYYMMDD.ext
	# stuff-YYY-MM-DD.ext

	# remove the extenstion
	base = filename.split('.')[0]

	# loop over the patterns, use the first that matches...
	for pattern in PATTERNS:
		for m in re.finditer(pattern, base):
			if m:
				return convert_to_datetime(m.group('year'), m.group('month'), m.group('day'))
	return None

def identify_and_copy_latest_file(args):
	# loop over files matching the source pattern
	# and index by datetime
	filedates = []
	for filename in glob.glob(args.src):
		dt = extract_datetime_from_filename(filename)
		if dt:
			# skip any files that don't match any patterns
			filedates.append((dt, filename))
	# copy the file
	if len(filedates) > 0:
		newest = sorted(filedates, reverse=True)[0]
		filename = newest[1]
		sys.stderr.write(f'Copying {filename} to {args.dest}...')
		shutil.copyfile(filename, args.dest)
		sys.stderr.write('OK.\n')
	else:
		sys.stderr.write('Error, could not identify latest file.\n')


if __name__ == '__main__':
	parser = argparse.ArgumentParser(prog=sys.argv[0], 
		description='Identify by date pattern and copy most recent file to a given location')
	parser.add_argument('-src', required=True)
	parser.add_argument('-dest', required=True)
	args = parser.parse_args()
	identify_and_copy_latest_file(args)
