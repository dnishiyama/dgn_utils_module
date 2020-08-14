def module_exists(module_name):
	try: __import__(module_name)
	except ImportError: return False
	else: return True

import sys, json, os, pymysql, re, logging, pdb, pytz, requests, pickle, math, urllib.parse, asks, trio
import subprocess, time, itertools, unicodedata, bs4, asks, trio
from collections import Counter, defaultdict
from enum import Enum, auto, IntEnum
from datetime import timedelta, date, datetime
from IPython.display import clear_output, Image, display
from typing import List, Tuple, Dict
from numbers import Number

if module_exists("nbslack"):
	import nbslack
	nbslack.notifying('dnishiyama', os.environ['SLACK_WEBHOOK'], error_handle=False)
	def notify(text='Work'): nbslack.notify(f"{text}")
else:
	logging.warning('Unable to load slack webhook')
	def notify(text=None): return

asks.init("trio")

print('08/04/20 dgnutils update loaded!')

# Use "python setup.py develop" in the directory to use conda develop to manage this file


# Utility Function {{{

#########################################  
########### UTILITY FUNCTIONS ###########
#########################################

########### TIME CONVERSIONS ############
def time_days_ago(days=0): return datetime.now(tz=pytz.timezone('US/Eastern')) - timedelta(days=days)
def time_ck_to_dt(time: str): return datetime.strptime(time, '%Y-%m-%d')
def time_dt_to_ck(dt): return datetime.strftime(dt, '%Y-%m-%d')
def time_created_to_dt(time: str): return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000Z')
def time_created_to_ck(time: str): return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d')
def time_mailchimp_to_dt(time: str): return datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
def time_mailchimp_to_ck(time: str): return datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# UNIX TIME
def time_current_unix(): return datetime_to_unix(datetime.now(tz=pytz.utc))
def time_dt_to_unix(dt): 
	if not dt.tzinfo: raise Exception("datetime must have tzinfo. Use datetime.now(tz=pytz.utc)")
	return int(dt.astimezone(pytz.utc).timestamp())
def time_unix_to_dt(unix): return datetime.utcfromtimestamp(unix).replace(tzinfo=pytz.utc)
def time_unix_to_str(unix, string_format="%Y-%m-%dT%H:%M:%S"): return datetime.strftime(datetime.utcfromtimestamp(unix).replace(tzinfo=pytz.utc), string_format)
def time_str_to_unix(string, string_format="%Y-%m-%d"): 
	""" Assumes that the string is local time """
	return datetime_to_unix(datetime.strptime(string, string_format).astimezone(pytz.utc))

def daterange(start_date, end_date):
	for n in range(int ((end_date - start_date).days)):
		yield start_date + timedelta(n)

#Legacy
def conv_time_1(time: str): logging.warning('conv_time_1 Deprecated; Use time_ck_to_dt'); return time_ck_to_dt(time);
def days_ago(days=0): logging.warning('Deprecated; Use prefix time_'); return time_days_ago(days=days)
def current_unix(): logging.warning('current_unix Deprecated; Use prefix time_'); return time_current_unix();
def datetime_to_unix(dt): logging.warning('datetime_to_unix Deprecated; Use time_dt_to_unix'); return time_dt_to_unix(dt)
def unix_to_datetime(unix): logging.warning('unix_to_datetime Deprecated; Use time_unit_to_dt'); return time_unix_to_dt(unix)
def unix_to_string(unix, string_format="%Y-%m-%dT%H:%M:%S"): logging.warning('unix_to_string Deprecated; Use time_unix_to_str'); return time_unix_to_str(unix, string_format=string_format)
def string_to_unix(string, string_format="%Y-%m-%d"): logging.warning('string_to_unix Deprecated; Use time_str_to_unix'); return time_str_to_unix(string, string_format=string_format)
def created_to_dt(time: str): logging.warning('created_to_dt Deprecated; Use prefix time_'); return time_created_to_dt(time);
def created_at_conv(time: str): logging.warning('created_at_conv Deprecated; Use time_created_to_ck'); return time_created_to_ck(time);
def mailchimp_conv(time: str): logging.warning('mailchimp_conv Deprecated; Use time_ck_to_dt'); return time_mailchimp_to_ck

# }}} Utility Function


########### LOGGING FUNCTIONS #############
def log_level(level=None):
	if type(level)==int: logging.getLogger().setLevel(level)
	elif level in ['d', 'debug']: logging.getLogger().setLevel(logging.DEBUG)
	elif level in ['i', 'info']: logging.getLogger().setLevel(logging.INFO)
	elif level in ['w', 'warn', 'warning', None]: logging.getLogger().setLevel(logging.WARNING) #default
	elif level in ['e', 'error']: logging.getLogger().setLevel(logging.ERROR)
	elif level in ['c', 'critical']: logging.getLogger().setLevel(logging.CRITICAL)
	elif level in ['f', 'fatal']: logging.getLogger().setLevel(logging.FATAL)
	else: raise Exception('Unrecognized log level', level)

########### TRACKING FUNCTIONS #############
def dgn_enum(_list:list):
	"""
	Use this function in the place of enumerate() to print the status
	"""
	for i,l in enumerate(_list):
		print(f'Progress {i+1}/{len(_list)}', end='\r')
		yield l


########### OTHER FUNCTIONS #############
def get_json_columns(dict_, columns=["id"]):
	return json.dumps({k:v for k,v in dict_.items() if k in columns})

def getInt(val, default=None):
    """
    try to get an int from val, return `default` if fails
    """
    try:
        return int(val)
    except ValueError as v:
        return default

# MYSQL Functions {{{

#########################################  
############ MYSQL FUNCTIONS ############
#########################################

def connect(db='ck_info', **kwargs):
	#if db in ['staging', 'stage', 'etymology_explorer_staging']: database = 'etymology_explorer_staging'
	#elif db in ['training', 'train', 'training_data']: database = 'training_data'
	#elif db in ['all', '', 'full', 'normal', 'etymology_explorer', 'live']: database = 'etymology_explorer'
	database = db;

	host = os.environ['RDS_CK_HOST'] if 'host' not in kwargs else kwargs['host']
	user = os.environ['RDS_CK_USER'] if 'user' not in kwargs else kwargs['user']
	password = os.environ['RDS_CK_PASSWORD'] if 'password' not in kwargs else kwargs['password']
	cursorclass = pymysql.cursors.Cursor if 'cursorclass' not in kwargs else kwargs['cursorclass']

	conn = pymysql.connect(user=user, password=password, host=host, database=database, cursorclass=cursorclass)#, ssl_disabled=True)
	cursor = conn.cursor(); cursor.execute('SET NAMES utf8mb4;')
	return conn, cursor

# cursor Helper functions
def quick_info(self, table='subscriber_data', **kwargs):
	options = ' WHERE 1=1 '
	for k,v in kwargs.items():
		options += f' AND {k}={repr(v)}'
	sql_stmt = f'SELECT * FROM {table} {options}'
	print(sql_stmt)
	amount = self.execute(sql_stmt)
	print(amount)
	if not amount: print([c[0] for c in self.description])
	return self.fetchone()
pymysql.cursors.DictCursor.quick_info = quick_info
pymysql.cursors.Cursor.quick_info = quick_info

def e(self, sql_stmt, values=None, many=False):
	execute_fn = self.executemany if many else self.execute # Choose executemany or execute
	
	rows = execute_fn(sql_stmt) if values==None else execute_fn(sql_stmt, values)
	if sql_stmt[:6] in ['DELETE', 'INSERT', 'UPDATE']: return rows
	return self.fetchall()
pymysql.cursors.DictCursor.e = e
pymysql.cursors.Cursor.e = e


def d(self, sql_stmt, values=None, many=False):
	"""
	Provides a way to return dictionary results regardless of cursor type
	"""
	execute_fn = self.executemany if many else self.execute # Choose executemany or execute
	
	rows = execute_fn(sql_stmt) if values==None else execute_fn(sql_stmt, values)
	if sql_stmt[:6] in ['DELETE', 'INSERT', 'UPDATE']: return rows
	if type(self) == pymysql.cursors.Cursor:
		return [{k:v for k,v in zip([d[0] for d in self.description], s)} for s in self.fetchall()]
	else:
		return self.fetchall()
pymysql.cursors.DictCursor.d = d
pymysql.cursors.Cursor.d = d

def dict_insert(self, data_list:list, table:str, batch_size:int=None):
	"""
	Insert a dictionary of data into mysql. On duplicate update. The keys must match the column names

	Parameters
	==========
	data_list (list(dict)): Must provide a list of dictionaries, where the dict keys match the col names
	table (str): the table to insert the data into
	batch_size (int): the size of the batches to use. If None, then will execute as one action

	Returns
	=======
	rows_updates (int): The number of rows updated
	"""
	# Should receive an array of dictionaries
	if type(data_list) not in [list, tuple]: raise TypeError('dict_insert must receive a list of dictionaries')
	if type(data_list[0]) != dict: raise TypeError('dict_insert must receive a list of dictionaries')

	columns = list(data_list[0].keys())
	values = [[d[c] for c in columns] for d in data_list]
	column_string = ", ".join(["`"+col+"`" for col in columns])
	variable_string = ", ".join(["%s"]*len(columns))
	duplicate_string = f'ON DUPLICATE KEY UPDATE {", ".join(["`"+c+"`=VALUES(`"+c+"`)" for c in columns])}'

	# Prep and execute statement
	sql_string = f'INSERT INTO {table} ({column_string}) VALUES ({variable_string}) {duplicate_string};'
	logging.debug(f'dict_insert {len(values)} values: {str([v for v in values[:2]])[:50]}')


	if not batch_size:
		self.executemany(sql_string, values)
	else:
		batches = math.ceil(len(values) / batch_size)
		logging.debug(f'number of batches: {batches}')
		for i in range(batches):
			logging.debug(f'in insert(), updating sql, iteration: {i}')
			values_batch = values[i*batch_size:(i+1)*batch_size]
			self.executemany(sql_string, values_batch)

pymysql.cursors.DictCursor.dict_insert = dict_insert
pymysql.cursors.Cursor.dict_insert = dict_insert

def array_insert(self, data_list:list, columns:list, table:str):
	"""
	Insert an array of data into mysql. On duplicate update. The keys must match the column names. Uses dict_insert

	Parameters
	==========
	data_list (list(dict)): Must provide a list of dictionaries, where the dict keys match the col names
	table (str): the table to insert the data into

	Returns
	=======
	rows_updates (int): The number of rows updated
	"""
	# Should receive an array of dictionaries
	if type(data_list) not in [list, tuple]: raise TypeError('array_insert must receive a list of lists. The parent element is not a list')
	if type(data_list[0]) not in [list, tuple]: raise TypeError('array_insert must receive a list of lists. The child elements are not lists')
	if not all(len(d) == len(columns) for d in data_list): raise AttributeError(f'column list len ({len(columns)}) must match each item in d ({len(data_list[0])})')
	
	data_list = [{c:d[i] for i,c in enumerate(columns)} for d in data_list]
	return dict_insert(self, data_list, table)
pymysql.cursors.DictCursor.array_insert = array_insert
pymysql.cursors.Cursor.array_insert = array_insert


# SQL commands {{{

def execute_sql(cursor, sql):
	"""Returns the fetchall() result after error catching"""
	data = None
	debug_sql = repr(re.sub(r"\s+", " ", sql))
	# Show one line for logging.INFO and the full text for logging.DEBUG
	if logging.getLogger().level == logging.DEBUG and len(debug_sql) > 80: 
		debug_sql=debug_sql[:80]+'...'
		logging.debug(f'Executing sql: {debug_sql}');
	try:
		cursor.execute(sql)
		data = cursor.fetchall()
	except mysql.connector.InternalError as ie:
		logging.warning('Cursor already had content, trying to empty and then execute again')
		cursor.fetchall()
		cursor.execute(sql)
		data = cursor.fetchall()
		if hasattr(cursor, 'description'):
			cursor.column_names = [t[0] for t in cursor.description]
	return data

# Old method before 7-16-20
# def insert(cursor, table, replace=False, ignore=False, many=False, batch_size=50000, **kwargs):
#	insert = 'REPLACE' if replace else 'INSERT'
#	ignore = 'IGNORE' if ignore else ''
#	columns = [str(k) for k,v in kwargs.items()]
#	col_text = '('+', '.join(columns)+')'
#	val_text = ', '.join(['%s']*len(kwargs))
#	sql_command = f'{insert} {ignore} INTO {table}{col_text} VALUES ({val_text})'
# 
#	if not many: #single insert
#		values = list(kwargs.values())
#		logging.debug(f'inserting {values} into {sql_command}')
#		cursor.execute(sql_command, values)
#	else: #multiple insert
#		values = list(zip(*kwargs.values())) #get each crosssection of arrays
#		batches = math.ceil(len(values) / batch_size)
#		logging.debug(f'number of batches: {batches}')
#		for i in range(batches):
#			logging.debug(f'in insert(), updating sql, iteration: {i}')
#			values_batch = values[i*batch_size:(i+1)*batch_size]
#			cursor.executemany(sql_command, values_batch)

def insert(cursor, table, replace=False, ignore=False, many=False, batch_size=50000, **kwargs):
	# print(kwargs)
	insert = 'REPLACE' if replace else 'INSERT'
	ignore = 'IGNORE' if ignore else ''
	columns = [str(k) for k,v in kwargs.items()]
	col_text = '('+', '.join(columns)+')'
	val_text = ', '.join(['%s']*len(kwargs))
	sql_command = f'{insert} {ignore} INTO {table}{col_text} VALUES ({val_text})'

	if not many: #single insert
		values = list(kwargs.values())
		logging.debug(f'inserting {values} into {sql_command}')
		cursor.e(sql_command, values)
	else:
		values = tuple(zip(*kwargs.values())) #get each crosssection of arrays
		batches = math.ceil(len(values) / batch_size)
		logging.debug(f'number of batches: {batches}')
		for i in range(batches):
			logging.debug(f'in insert(), updating sql, iteration: {i}')
			values_batch = values[i*batch_size:(i+1)*batch_size]
			cursor.executemany(sql_command, values_batch)

def update(cursor, table, entry_id, **kwargs):		
	update_text = ', '.join([k+'=%s' for k in kwargs])
	sql_command = f'UPDATE {table} SET {update_text} WHERE entry_id={entry_id}'
	values = list(kwargs.values())
	logging.debug(f'updating: {sql_command} with {values}')
	cursor.execute(sql_command, values)


# }}} SQL commands

# array_insert tests
#table='TEST_subscriber_data_list'
#data_list=[[803,'', 'test', None, None, None, None, None, None, None, None,None,None,None,None,None],
#	  [804,'', 'test', None, None, None, None, None, None, None, None,None,None,None,None,None],
#	  [805,'', 'test', None, None, None, None, None, None, None, None,None,None,None,None,None]]
#columns = ['id', 'state', 'first_name', 'email_address', 'created_at', 'fb_audience_source', 'loc_city', 'loc_state', 'loc_lat',
# 'loc_lng', 'lead_opt-in', 'lead_source', 'mailchimp_sub_date', 'stsbc_evg_launch_deadline', 'subscribe_date', 'unsubscribe_date']
#cursor.array_insert(data_list, columns, table)

# Convenience function since I changed the name recently
def print_array(array:list):
	return mysql_array(array)

def mysql_array(array:list):
	if not array: return '(NULL)'
	return '(' + ', '.join([repr(a) for a in array]) + ')'

def database_sizes(cursor, exclude_zero=True):
	db_sizes={}
	for table in [list(v.values())[0] for v in cursor.d('SHOW TABLES;')]:
		db_sizes[table] = cursor.d(f'SELECT COUNT(*) as c FROM {table};')[0]['c']
	if exclude_zero:
		db_sizes = {k:v for k,v in db_sizes.items() if v != 0}
	return db_sizes

def copy_tables(cursor, copy_from_database, copy_to_database):
	"""
	drop all tables
	Then insert them from the other source based on the `SHOW CREATE TABLE {table}`
	"""
	database_in_use = cursor.e('SELECT DATABASE() FROM DUAL;')[0][0]
	create_table_stmts=[]
	cursor.e(f'DROP DATABASE IF EXISTS {copy_to_database}')
	cursor.e(f'CREATE DATABASE {copy_to_database}')
	tables = [d[0] for d in cursor.e(f'SHOW TABLES FROM {copy_from_database};')]
	logging.info(f'Recreating all tables')
	cursor.e(f'USE {copy_to_database}')
	for table in tables:
		create_table_stmt = cursor.d(f'SHOW CREATE TABLE {copy_from_database}.{table}')[0]['Create Table']
		logging.debug(f'Recreating table {table}')
		cursor.e(create_table_stmt)
	cursor.e(f'USE {database_in_use}')


def refresh_tables(cursor, exclude:list):
	"""
	drop all tables, except those included in `exclude`
	Then reinstert them based on the `SHOW CREATE TABLE statements`
	"""
	create_table_stmts=[]
	tables = [d[0] for d in cursor.e('SHOW TABLES;')]
	logging.info(f'Recreating all tables EXCEPT {exclude}')
	for table in tables:
		if table not in exclude: 
			create_table_stmt = cursor.d(f'SHOW CREATE TABLE {table}')[0]['Create Table']
			logging.debug(f'Recreating table {table}')
			cursor.e(f'DROP TABLE IF EXISTS {table}')
			cursor.e(create_table_stmt)

# MYSQL Functions }}}
