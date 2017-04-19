'''
v20170419a
Example:

python /Users/ereiman/github/aws_admin/data_loader.py -d '/Users/ereiman/tmp/complete' -w 'clarity' -df True
python /efs/aws_admin/data_loader.py -d '/efs/dummydata/complete' -w 'clarity' -df n -m 2
nohup python /efs/aws_admin/data_loader.py -d '/efs/dummydata/complete' -w 'clarity' -df y -m 250 &

#files to process:
ls -l /efs/dummydata/complete | wc -l
mv /efs/dummydata/complete/in_process/S* /efs/dummydata/complete
'''

import argparse
from datetime import datetime
from datetime import timedelta
import json
import os
import random
import time
import sys
import re

file_types = ['Sales','Customer','Product']

def get_command_line_arguments():
	parser = argparse.ArgumentParser()
	#REQUIRED ARGUMENTS:  -d (directory), -w (vsql_password)
	parser.add_argument("-d", "--directory", help="Directory containing files")
	parser.add_argument("-cd", "--child_directory", help="Child directory containing files while processing", default='in_process')
	parser.add_argument("-df", "--delete_file_after_load", help="Delete file after loaded?  (y/n)", default='n')
	parser.add_argument("-m", "--max_number_of_files_to_process", help="Maximum # of files to process in this run", default=10, type=int)
	parser.add_argument("-u", "--vsql_user", help="vsql_user", default='dbadmin')
	parser.add_argument("-w", "--vsql_pwd", help="vsql_pwd")
	args = parser.parse_args()
	return args

def create_subdirectory_for_processing(directory,child_directory='in_process'):
	new_directory = os.path.join(directory,child_directory)
	if not os.path.exists(new_directory):
		try:
			os.makedirs(new_directory)
		except:
			print("Error: create_subdirectory_for_processing")

def list_files_to_process(directory,child_directory='in_process',max_number_of_files_to_process=10):
	print('Max files to process: {}'.format(max_number_of_files_to_process))
	files_to_process = []
	i=1
	files = os.listdir(directory)
	for file in files:
		if i > int(max_number_of_files_to_process):
			break
		file_base = re.match(r'(^[a-zA-Z]+)',file)
		if file_base and file_base.group(0) in file_types:
			table_name = file_base.group(0)
			try:
				src = os.path.join(directory,file)
				dest = os.path.join(directory,child_directory,file)
				os.rename(src, dest)
				files_to_process.append((
					os.path.join(directory,child_directory)
					, file
					, table_name
					))
				i+=1
			except:
				print("Error: list_files_to_process - could not move file")
				print "Unexpected error:", sys.exc_info()[0]
				raise
	return files_to_process

def load_data(files_to_process,vsql_user,vsql_pwd,delete_file_after_load):
	for file_tuple in files_to_process:
		directory = file_tuple[0]
		file = file_tuple[1]
		table_name = file_tuple[2]
		directory_and_file = os.path.join(directory,file)
		dat=time.strftime("%Y-%m-%d %H:%M:%S")
		print '{dat} - Loading file {directory_and_file}'.format(directory_and_file=directory_and_file,dat=dat)
		sql = 'copy {table_name} from local \'{directory_and_file}\' direct;'.format(table_name=table_name,directory_and_file=directory_and_file)
		os_cmd = '/opt/vertica/bin/vsql -U {vsql_user} -w {vsql_pwd} -c "{sql}"'.format(vsql_user=vsql_user,vsql_pwd=vsql_pwd,sql=sql)
		try:
			print os_cmd
			os.system(os_cmd)
		except:
			print 'ERROR - did not load successfully'
		if delete_file_after_load == 'y':
			try:
				os.remove(directory_and_file)
			except:
				print "Unexpected error - load_data:", sys.exc_info()[0]


def main():
	args = get_command_line_arguments()
	create_subdirectory_for_processing(args.directory,args.child_directory)
	files_to_process = list_files_to_process(
		directory=args.directory
		,child_directory=args.child_directory
		,max_number_of_files_to_process=args.max_number_of_files_to_process
		)
	load_data(files_to_process,args.vsql_user,args.vsql_pwd,args.delete_file_after_load)
		

if (__name__ == '__main__'):
    main()

