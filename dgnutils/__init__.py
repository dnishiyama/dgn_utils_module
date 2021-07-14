def module_exists(module_name):
	try: __import__(module_name)
	except ImportError: return False
	else: return True

import sys
import json
import os
import pymysql
import re
import logging
import pdb
import pytz
import requests
import pickle
import math
import urllib.parse
import asks
import trio
import subprocess
import time
import itertools
import unicodedata
import bs4
import random
import warnings
from collections import Counter, defaultdict
from enum import Enum, auto, IntEnum
from datetime import timedelta, date, datetime
from IPython.display import clear_output, Image, display
from typing import List, Tuple, Dict
from numbers import Number
from pathlib import Path

if module_exists("nbslack"):
	import nbslack
	nbslack.notifying('dnishiyama', os.environ['SLACK_WEBHOOK'], error_handle=False)
	def notify(text='Work'): nbslack.notify(f"{text}")
else:
	logging.warning('Unable to load slack webhook')
	def notify(text=None): return

# asks.init('trio')
print('7/14/21 dgnutils update loaded! Added getPop')

# Use "python setup.py develop" in the directory to use conda develop to manage this file


# Utility Function {{{

#########################################  
########### UTILITY FUNCTIONS ###########
#########################################

def shuffle_return(l:list, seed=None):
	""" shuffles list in place and returns it"""
	if type(l) != list: raise Exception('Must provide a list!')
	if seed != None:
		random.Random(seed).shuffle(l)
	else:
		random.shuffle(l)
	return l

def color(text, color):
	e = '\x1b[0m'
	c = {
		'red': '\x1b[1;31m',
		'red_light':'\x1b[31m',
		'teal':'\x1b[1;96m',
		'teal_light': '\x1b[96m',
		'purple': '\x1b[1;95m',
		'purple_light': '\x1b[95m',
		'blue': '\x1b[1;94m',
		'blue_light': '\x1b[94m',
		'black': '\x1b[1;30m',
	}
	if not color in c: raise Exception(f'No color for {color}')
	return f'{c[color]}{text}{e}'

########### TIME CONVERSIONS ############
def time_days_ago(days=0): return datetime.now(tz=pytz.timezone('US/Eastern')) - timedelta(days=days)
def time_ck_to_dt(time: str): return datetime.strptime(time, '%Y-%m-%d')
def time_dt_to_ck(dt): return datetime.strftime(dt, '%Y-%m-%d')
def time_created_to_dt(time: str): return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000Z')
def time_created_to_ck(time: str): return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d')
def time_mailchimp_to_dt(time: str): return datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
def time_mailchimp_to_ck(time: str): return datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# UNIX TIME
def time_current_unix(): return time_dt_to_unix(datetime.now(tz=pytz.utc))
def time_dt_to_unix(dt): 
	if not dt.tzinfo: raise Exception("datetime must have tzinfo. Use datetime.now(tz=pytz.utc)")
	return int(dt.astimezone(pytz.utc).timestamp())
def time_unix_to_dt(unix): return datetime.utcfromtimestamp(unix).replace(tzinfo=pytz.utc)
def time_unix_to_str(unix, string_format="%Y-%m-%dT%H:%M:%S"): return datetime.strftime(datetime.utcfromtimestamp(unix).replace(tzinfo=pytz.utc), string_format)
def time_str_to_unix(string, string_format="%Y-%m-%d"): 
	""" Assumes that the string is local time """
	return time_dt_to_unix(datetime.strptime(string, string_format).astimezone(pytz.utc))
def time_str_to_s_ts(string, string_format="%Y-%m-%d"): 
	return time_dt_to_unix(datetime.strptime(string, string_format).astimezone(pytz.utc))
def time_str_to_ms_ts(string, string_format="%Y-%m-%d"): 
	return time_dt_to_unix(datetime.strptime(string, string_format).astimezone(pytz.utc))*1000
def time_ts_to_str(ts):
	""" handles ms or s """
	if ts > 253402300799: ts /= 1000
	return time_unix_to_str(ts)



def daterange(start_date, end_date):
	for n in range(int ((end_date - start_date).days)):
		yield start_date + timedelta(n)

### Dictionary helpers
def dict_head(dictionary, num_items): 
	return {k:v for i,(k,v) in enumerate(dictionary.items()) if i < num_items}

def first_value(dictionary, none_item=None): 
	return next(iter(dictionary.values()), none_item)

def first_key(dictionary, none_item=None): 
	return next(iter(dictionary.keys()), none_item)

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

def pprint(v, indent=2):
	print(json.dumps(v, indent=indent))

def getInt(val, default=None):
	"""
	try to get an int from val, return `default` if fails
	"""
	try:
		return int(val)
	except (ValueError, TypeError):
		return default

def getFloat(val, default=None):
	"""
	try to get an float from val, return `default` if fails
	"""
	try:
		return float(val)
	except ValueError:
		return default

def getPop(val, default=None):
	"""
	Try to use next(iter(val, default)) on a list
	"""
	return next(iter(val), default)

def permutations(l):
	comb = []
	for r in range(2,len(l)+1):
		comb += list(itertools.permutations(l, r))
	return comb

def combinations(l):
	comb = []
	for r in range(2,len(l)+1):
		comb += list(itertools.combinations(l, r))
	return comb

def nested_update(d0, d1):
	"""
	Parameters
		d0: the dictionary to update
		d1: the dictionary values to update into d0
	Returns
		d0: The updated dictionary (it is also updated in place)
	"""    
	for k, v in d1.items():
		if isinstance(v, dict):
			d0[k] = nested_update(d0.get(k, {}), v)
		else:
			d0[k] = v
	return d0
	# a = {1:{2:{3}}}
	# b = {1:{3:{4}}}
	# c = nested_update(a, b)
	# a,b,c

# reExport = re.compile(r'^\s*export ([A-Z0-9_]+)=[\'\"](.*?)[\'\"]\s*$')
reExport = re.compile(r'^[^#]*?export ([A-Z0-9_]+)=[\'\"](.*?)[\'\"]\s*$') # more clear check for comments
def extract_sh_exports(file_path):
	"""
	Take a shell file and extract the exports. Returns a dictionary, which can be updated onto os.environ
	"""
	# '/gdrive/My Drive/development/.dgn_secrets'
	exports = {}
	with open(file_path, 'r') as file:
		for line in file:
			export_match = reExport.match(line)
			if export_match:
				exports[export_match.group(1)] = export_match.group(2)
	return exports

def test_extract_sh_exports():
	assert reExport.match("export RDS_CK_HOST='test token!@#ASDFasdf123[]''")
	assert reExport.match("export RDS_CK_HOST='test token!@#ASDFasdf123[]''").group(1) == 'RDS_CK_HOST'
	assert reExport.match("export RDS_CK_HOST='test token!@#ASDFasdf123[]''").group(2) == 'test token!@#ASDFasdf123[]\''
	assert reExport.match("export NOTION_TOKEN_V2='test token!@#ASDFasdf123[]\''")
	assert not reExport.match('#export REDDIT_MESSAGER_CLIENT_ID="test token!@#ASDFasdf123[]"')
	assert not reExport.match('# export REDDIT_MESSAGER_CLIENT_SECRET="test token!@#ASDFasdf123[]\'"')

# MYSQL Functions {{{

#########################################  
############ MYSQL FUNCTIONS ############
#########################################

def connect(db = 'staging', **kwargs):
	if db in ['backup', 'back']: database = 'etymology_explorer_backup'
	elif db in ['training', 'train']: database = 'training_data'
	elif db in ['test', 'testing']: database = 'etymology_explorer_test'
	elif db in ['development', 'dev']: database = 'etymology_explorer_dev'
	elif db in ['staging', 'stage']: database = 'etymology_explorer_staging'
	elif db in ['prod', 'production']: database = 'etymology_explorer_prod'
	else: database = db
	user = os.environ['ETY_USER'] if 'user' not in kwargs else kwargs['user']
	password = os.environ['ETY_PASSWORD'] if 'password' not in kwargs else kwargs['password']
	host = os.environ['ETY_HOST'] if 'host' not in kwargs else kwargs['host']
	# user = os.environ['MYSQL_DB_USER'] if 'user' not in kwargs else kwargs['user']
	# password = os.environ['MYSQL_DB_PASSWORD'] if 'password' not in kwargs else kwargs['password']
	cursorclass = pymysql.cursors.Cursor if 'cursorclass' not in kwargs else kwargs['cursorclass'] #pymysql.cursors.DictCursor

	conn = pymysql.connect(user=user, password=password, host=host, database=database, cursorclass=cursorclass)
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

def dict_insert(self, data_list:'list[dict]', table:str, batch_size:int=None):
	"""
	Insert a dictionary of data into mysql. 
	Allows updates via (On duplicate update)
	The keys must match the column names. If a key is missing, the value defaults to None (NULL) in SQL

	Parameters
		data_list (list(dict)): Must provide a list of dictionaries, where the dict keys match the col names
		table (str): the table to insert the data into
		batch_size (int): the size of the batches to use. If None, then will execute as one action

	Returns
		rows_updates (int): The number of rows updated
	"""
	# Should receive an array of dictionaries
	if type(data_list) not in [list, tuple]: raise TypeError('dict_insert must receive a list of dictionaries')
	if len(data_list) == 0: 
		warnings.warn('Nothing to insert in dict_insert!')
		return
	if type(data_list[0]) != dict: raise TypeError('dict_insert must receive a list of dictionaries')

	# updated columns fn on 1-5-21 to allow different columns in each row
	fields = set(c['Field'] for c in self.d(f'DESCRIBE {table}'))
	columns = set(k for e in data_list for k in list(e))
	unused_columns = columns - fields
	if unused_columns:
		print('columns:', columns)
		print('fields:', fields)
		print('unused_columns:', unused_columns)
		raise Exception(f'Cannot dict_insert with unused columns: {unused_columns}')
	values = [[data_item.get(c) for c in columns] for data_item in data_list]
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

def dict_delete(self, data_list:'list[dict]', table:str, batch_size:int=1000):
	"""
	Delete rows from mysql based on a dictionary of data 
	The keys must match the column names

	Parameters
		data_list (list(dict)): Must provide a list of dictionaries, where the dict keys match the col names
		table (str): the table to insert the data into
		batch_size (int): default 1000, the size of the batches to use. If None, then will execute as one action

	Returns
		rows_updates (int): The number of rows deleted
	"""
	# Should receive an array of dictionaries
	if type(data_list) not in [list, tuple]: raise TypeError('dict_insert must receive a list of dictionaries')
	if len(data_list) == 0: 
		logging.warning('Nothing to insert!')
		return
	if type(data_list[0]) != dict: raise TypeError('dict_delete must receive a list of dictionaries')

	# updated columns fn on 1-5-21 to allow different columns in each row
	columns = list(set(k for e in data_list for k in list(e)))
	if any(set(data_item) != set(columns) for data_item in data_list):
		raise Exception('All rows must have the same keys!')
	column_string = " AND ".join(["`"+col+"`=%s" for col in columns])
	values = [[data_item.get(c) for c in columns] for data_item in data_list]

	# Prep and execute statement
	sql_string = f'DELETE FROM {table} WHERE {column_string};'
	logging.debug(f'dict_delete sql_string: {sql_string}')
	logging.debug(f'dict_delete {len(values)} values: {str([v for v in values[:2]])[:50]}')

	if not batch_size:
		self.executemany(sql_string, values)
	else:
		batches = math.ceil(len(values) / batch_size)
		logging.debug(f'number of batches: {batches}')
		for i in range(batches):
			logging.debug(f'in delete(), updating sql, iteration: {i}')
			values_batch = values[i*batch_size:(i+1)*batch_size]
			self.executemany(sql_string, values_batch)

pymysql.cursors.DictCursor.dict_delete = dict_delete
pymysql.cursors.Cursor.dict_delete = dict_delete

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
	if not all(len(data_item) == len(columns) for data_item in data_list): raise AttributeError(f'column list len ({len(columns)}) must match each item in d ({len(data_list[0])})')
	
	data_list = [{c:data_item[i] for i,c in enumerate(columns)} for data_item in data_list]
	return dict_insert(self, data_list, table)
pymysql.cursors.DictCursor.array_insert = array_insert
pymysql.cursors.Cursor.array_insert = array_insert

def db(self):
	""" check the current db via DATABASE() FROM DUAL """
	return self.d('SELECT DATABASE() FROM DUAL')[0]['DATABASE()']
pymysql.cursors.DictCursor.db = db
pymysql.cursors.Cursor.db = db

def cts(self, table):
	""" check create table statement of a table """
	print(self.d(f'SHOW CREATE TABLE {table}')[0]['Create Table'])
pymysql.cursors.DictCursor.cts = cts
pymysql.cursors.Cursor.cts = cts

# SQL commands {{{

def execute_sql(cursor, sql):
	"""Returns the fetchall() result after error catching"""
	data = None
	debug_sql = repr(re.sub(r"\s+", " ", sql))
	# Show one line for logging.INFO and the full text for logging.DEBUG
	if logging.getLogger().level == logging.DEBUG and len(debug_sql) > 80: 
		debug_sql=debug_sql[:80]+'...'
		logging.debug(f'Executing sql: {debug_sql}')

	cursor.execute(sql)
	data = cursor.fetchall()
	return data

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

def update(cursor, table, **kwargs):		
	raise Exception('not sure this works, do I need a WHERE clause?')
	update_text = ', '.join([k+'=%s' for k in kwargs])
	sql_command = f'UPDATE {table} SET {update_text}'
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

def database_sizes(cursor, exclude_zero=True, estimate=False):
	db_sizes={}
	if estimate:
		database = cursor.e('SELECT DATABASE() FROM DUAL;')[0][0] 
		sql_stmt = f"""
		SELECT table_name AS "Table",
		ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)"
		FROM information_schema.TABLES
		WHERE table_schema = "{database}"
		ORDER BY (data_length + index_length) DESC;
		"""
		return { d[0] : f"{d[1]} MB" for d in cursor.e(sql_stmt) }

	for table in [list(v.values())[0] for v in cursor.d('SHOW TABLES;')]:
		db_sizes[table] = cursor.d(f'SELECT COUNT(*) as c FROM {table};')[0]['c']

	if exclude_zero: return {k:v for k,v in db_sizes.items() if v != 0}

	return db_sizes

def copy_tables(cursor, copy_from_database, copy_to_database=None, contents=[]):
	"""
	drop all tables (by dropping the database)
	Then insert them from the other source based on the `SHOW CREATE TABLE {table}`
	:param copy_from_database: The database to replicate
	:param copy_to_database: if not provided, then defaults to the database in use
	:param contents: list of tables to copy over content as well
	"""
	database_in_use = cursor.e('SELECT DATABASE() FROM DUAL;')[0][0]
	if not copy_to_database:
		copy_to_database = database_in_use
	cursor.e(f'DROP DATABASE IF EXISTS {copy_to_database}')
	cursor.e(f'CREATE DATABASE {copy_to_database}')
	tables = [d[0] for d in cursor.e(f'SHOW TABLES FROM {copy_from_database};')]
	logging.info(f'Recreating all tables')
	cursor.e(f'USE {copy_to_database}')
	for table in tables:
		create_table_stmt = cursor.d(f'SHOW CREATE TABLE {copy_from_database}.{table}')[0]['Create Table']
		logging.debug(f'Recreating table {table}')
		cursor.e(create_table_stmt)
		if table in contents:
			cursor.e(f'INSERT INTO {copy_to_database}.{table} SELECT * FROM {copy_from_database}.{table}')
	cursor.e(f'USE {database_in_use}')

def refresh_table(cursor, table:str, keep_contents=False):
	if keep_contents:
		data = cursor.d(f'SELECT * FROM {table}')
	create_table_stmt = cursor.d(f'SHOW CREATE TABLE {table}')[0]['Create Table']
	logging.debug(f'Recreating table {table}')
	cursor.e(f'DROP TABLE IF EXISTS {table}')
	cursor.e(create_table_stmt)
	if keep_contents:
		cursor.dict_insert(data, table)

def refresh_tables(cursor, exclude:list, keep_contents:'list or boolean'=[]):
	"""
	drop all tables, except those included in `exclude`
	Then reinstert them based on the `SHOW CREATE TABLE statements`
	:param keep_contents: list of tables to copy over content as well
	"""
	tables = [d[0] for d in cursor.e('SHOW TABLES;')]
	logging.info(f'Recreating all tables EXCEPT {exclude}')
	for table in tables:
		if table in exclude: continue
		refresh_table(cursor, table, keep_contents == True or table in keep_contents)

def restore_missing_tables(cursor, source_database):
	"""
	if the database_to_check is missing any tables from source_database, then copy it over
	Then insert them from the other source based on the `SHOW CREATE TABLE {table}`
	"""
	database_in_use = cursor.e('SELECT DATABASE() FROM DUAL;')[0][0]
	existing_tables = [t[0] for t in cursor.e('SHOW TABLES;')]
	all_tables = [t[0] for t in cursor.e(f'SHOW TABLES FROM {source_database};')]
	missing_tables = [t for t in all_tables if t not in existing_tables]
	for table in missing_tables:
		create_table_stmt = cursor.d(f'SHOW CREATE TABLE {source_database}.{table}')[0]['Create Table']
		logging.debug(f'Recreating table {table}')
		cursor.e(f'USE {database_in_use}')
		cursor.e(create_table_stmt)
	cursor.e(f'USE {database_in_use}')

def close_connections(cursor, db_name):
	ids = [
		p['Id'] 
		for p in cursor.d('SHOW PROCESSLIST;') 
		if p['db']==db_name
		and (not p['Info'] or 'SHOW PROCESSLIST' not in p['Info'])
	]
	sql_stmts = [f'KILL {id};' for id in ids]; sql_stmts
	for sql_stmt in sql_stmts:
		cursor.e(sql_stmt)
	logging.info(f'Closed {len(sql_stmts)} connections to {db_name}')

def mysql_create_table_stmts(cursor, db:str):
	"""
	Get create table statement from a database
	Returns:
		stmts: dict {table: stmt, table2: stmt2, ...}
	"""
	create_table_stmts={}
	tables = [d[0] for d in cursor.e(f'SHOW TABLES FROM {db};')]
	for table in tables:
		create_table_stmts[table] = cursor.d(f'SHOW CREATE TABLE {db}.{table}')[0]['Create Table'].split('\n')
	return create_table_stmts

def mysql_compare_create_table_stmts(
		cursor, 
		stmts0:'dict[table:stmt]', 
		stmts1:'dict[table:stmt]', 
		name0:str='database0',
		name1:str='database1',
	):
	"""
	See the differences between databases in table structure
	Parameters
		name0: name of the first database
		name1: name of the second database
	Return
		databases_extras (dict): dict containing the extras that a database HAS
		{ 
			database0:{ 
				table1:[line1, line2] ,
				table2:[] #entire table 
			}
			database1:{ 
				table3:[] 
			}
		}
	"""
	database_differences = {name0:{},name1:{}}
	create_table_stmts={0:stmts0, 1:stmts1}

	only_db_0_tables = list(set(create_table_stmts[0]) - set(create_table_stmts[1]))
	only_db_1_tables = list(set(create_table_stmts[1]) - set(create_table_stmts[0]))

	for table in only_db_0_tables:
		database_differences[name0][table]=[]
	for table in only_db_1_tables:
		database_differences[name1][table]=[]

	print(color(f'"{name0}"-only tables {only_db_0_tables}', 'purple_light'))
	print(color(f'"{name1}"-only tables {only_db_1_tables}', 'blue_light'))
	print()
		
	matching_tables = set(create_table_stmts[0]) & set(create_table_stmts[1])
	for table in matching_tables:
		if create_table_stmts[0][table] != create_table_stmts[1][table]:
			print(f'"{table}" differences')
			for line in create_table_stmts[0][table]:
				if line not in create_table_stmts[1][table]:
					database_differences[name0].setdefault(table,[]).append(line)
					print(color(f'{name0}: {line}', 'purple_light'))
			for line in create_table_stmts[1][table]:
				if line not in create_table_stmts[0][table]:
					database_differences[name1].setdefault(table,[]).append(line)
					print(color(f'{name1}: {line}', 'blue_light'))
			print()
	return database_differences

def compare_databases(
		cursor, 
		database0:str, 
		database1:str, 
		create_table_stmts_override:dict={}, 
	):
	"""
	See the differences between databases in table structure
	Parameters
		database0: name of the first database
		database1: name of the second database
		create_table_stmts_override: optional dict of create_table_stmts to override, 
		updates the default by nested_update
			{0: {table: CREATE TABLE stmt}, ..., 1: {} }
	Return
		databases_extras (dict): dict containing the extras that a database HAS
		{ 
			database0:{ 
				table1:[line1, line2] ,
				table2:[] #entire table 
			}
			database1:{ 
				table3:[] 
			}
		}
	"""
	database_differences = {database0:{},database1:{}}
	name0 = ''.join([d for i,d in enumerate(database0) if len(database1) > i and database1[i] != d])
	name1 = ''.join([d for i,d in enumerate(database1) if len(database0) > i and database0[i] != d])
	create_table_stmts={0:{}, 1:{}}
	for i,db in enumerate(d for d in [database0, database1] if d):
		tables = [d[0] for d in cursor.e(f'SHOW TABLES FROM {db};')]
		for table in tables:
			create_table_stmts[i][table] = cursor.d(f'SHOW CREATE TABLE {db}.{table}')[0]['Create Table'].split('\n')

	create_table_stmts = nested_update(create_table_stmts, create_table_stmts_override)

	only_db_0_tables = list(set(create_table_stmts[0]) - set(create_table_stmts[1]))
	only_db_1_tables = list(set(create_table_stmts[1]) - set(create_table_stmts[0]))

	for table in only_db_0_tables:
		database_differences[database0][table]=[]
	for table in only_db_1_tables:
		database_differences[database1][table]=[]

	print(color(f'"{name0}"-only tables {only_db_0_tables}', 'purple_light'))
	print(color(f'"{name1}"-only tables {only_db_1_tables}', 'blue_light'))
	print()
		
	matching_tables = set(create_table_stmts[0]) & set(create_table_stmts[1])
	for table in matching_tables:
		if create_table_stmts[0][table] != create_table_stmts[1][table]:
			print(f'"{table}" differences')
			for line in create_table_stmts[0][table]:
				if line not in create_table_stmts[1][table]:
					database_differences[database0].setdefault(table,[]).append(line)
					print(color(f'{name0}: {line}', 'purple_light'))
			for line in create_table_stmts[1][table]:
				if line not in create_table_stmts[0][table]:
					database_differences[database1].setdefault(table,[]).append(line)
					print(color(f'{name1}: {line}', 'blue_light'))
			print()
	return database_differences

def store_database_structure(cursor, path=None):
	if not path:
		path = Path(os.environ['ETY_BACKEND_PATH'])/'data'/'wiktionary'/'table_structure.mysql'
	
	with open(path, 'w+') as f:
		database_in_use = cursor.e('SELECT DATABASE() FROM DUAL;')[0][0]
		tables = [d[0] for d in cursor.e(f'SHOW TABLES FROM {database_in_use};')]
		logging.debug(f'writing tables {tables}')
		create_table_stmts = [cursor.d(f'SHOW CREATE TABLE {database_in_use}.{table}')[0]['Create Table'] for table in tables]
		f.write('\n'.join(create_table_stmts))
		logging.info(f'Wrote {database_in_use} structure to {path}')

def overwrite_database_structure(cursor, database, path=None):
	"""
	"""
	database_in_use = cursor.db()
	if database_in_use and database != database_in_use:
		raise Exception(f'Unsure which database to reset! {database} given, {database_in_use} in use')
	elif not database_in_use:
		database_in_use = database
		print(f'No database in use. Using given {database}')
	if input(f'This will overwrite database: {database_in_use}, continue? [y/N]') != 'y': raise Exception()
	logging.info(f'Dropping database {database_in_use}')
	cursor.e(f'DROP DATABASE IF EXISTS {database_in_use}')
	cursor.e(f'CREATE DATABASE {database_in_use}')
	cursor.e(f'USE {database_in_use}')

	create_table_stmts = load_create_table_commands_from_structure_file(path=path)
	for create_table_stmt in create_table_stmts.values():
		cursor.e(''.join(create_table_stmt))

RE_TABLE_NAME = r"CREATE +TABLE +`?(\w+)`? +\("
def load_create_table_commands_from_structure_file(path=None):
	""" load table_structure.mysql into a list of create table commands """
	create_table_stmts={}
	if not path:
		path = Path(os.environ['ETY_BACKEND_PATH'])/'data'/'wiktionary'/'table_structure.mysql'
	with open(path, 'r') as f:
		create_table_cmd = []
		for line in f.readlines():
			line = line.rstrip()
			match = re.match(RE_TABLE_NAME, line)
			if match and create_table_cmd:
				create_table_stmts[table] = create_table_cmd 
				table = match.group(1) # Actually is the NEXT table
				create_table_cmd = [line]
			else:
				create_table_cmd.append(line)
				if match:
					table = match.group(1) # Actually is the NEXT table
		create_table_stmts[table] = create_table_cmd 
	return create_table_stmts

# MYSQL Functions }}}

class Timer(object):
	"""
	Class to time
	"""
	def __init__( self ):
		self.start_times = {}
		self.time_accumulations = {}

	def start(self, category):
		self.start_times[category] = time.time()

	def end(self, category):
		self.time_accumulations.setdefault(category, 0)
		self.time_accumulations[category] += time.time() - self.start_times[category]

	def log(self):
		return ', '.join(f"{k}:{round(v,1)}" for k,v in self.time_accumulations.items())


# Firebase utilities
def fb_date(fb_data:dict):
    """ Firebase convert a normal dict into readable dates"""
    for k,v in fb_data.items():
        if type(v) == dict:
            fb_data[k] = fb_date(v)
        elif type(v) == int and v > 1000000000:
            fb_data[k] = time_ts_to_str(v)
    return fb_data
