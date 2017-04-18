import argparse
from datetime import datetime
from datetime import timedelta
import json
import os
import random
import time
import sys
import re

delete_after_loaded = False
vsql_user = 'dbadmin'
vsql_pwd = 'clarity'
file_types = ['sales','customer','product']
delete_file_after_load = True

files = os.listdir(".")
for file in files:
	file_base = re.match(r'(^[a-zA-Z]+)',file)
	if file_base and file_base.group(0) in file_types:
		table_name = file_base.group(0)
		dat=time.strftime("%Y-%m-%d %H:%M:%S")
		print '{dat} - Loading file {file_to_load}'.format(file_to_load=file,dat=dat)
		sql = 'copy {table_name} from local \'{file}\' direct;'.format(table_name=table_name,file=file)
		os_cmd = '/opt/vertica/bin/vsql -U {vsql_user} -w {vsql_pwd} -c "{sql}"'.format(vsql_user=vsql_user,vsql_pwd=vsql_pwd,sql=sql)
		try:
			#print os_cmd
			os.system(os_cmd)
		except:
			print 'ERROR - did not work'
		if delete_file_after_load:
			os.remove(file)

