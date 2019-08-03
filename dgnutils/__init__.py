import json, os, nbslack, pymysql, re, logging, pdb
from enum import Enum, auto
from datetime import timedelta, date, datetime
from IPython.display import clear_output

#Tracing
# pdb.set_trace()

#########################################  
########### UTILITY FUNCTIONS ###########
#########################################

########### TIME CONVERSIONS ############
def created_at_conv(time: str): return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d')
def mailchimp_conv(time: str): return datetime.strptime(time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
def conv_time_1(time: str): return datetime.strptime(time, '%Y-%m-%d')
def conv_time_2(time:str):
    """ Convert a string from mailchimp (probably excel) into the format stored in ConvertKit """
    return datetime.strptime(time, '%m/%d/%y %H:%M').strftime('%Y-%m-%dT%H:%M:%S.000Z')
def conv_created_at_to_subscribe_date(created_at:str): return created_at_conv(created_at); # Legacy function
    
def get_json_columns(dict_, columns=["id"]):
    return json.dumps({k:v for k,v in dict_.items() if k in columns})

########### SLACK FUNCTIONS #############
# Slack notification code
nbslack.notifying('dnishiyama', os.environ['SLACK_WEBHOOK'], error_handle=False)
def notify(text='Work'): nbslack.notify(f"{text}")

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
    cursorclass = pymysql.cursors.DictCursor if 'cursorclass' not in kwargs else kwargs['cursorclass']

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

def e(self, sql_stmt, values=None, many=False):
    execute_fn = self.executemany if many else self.execute # Choose executemany or execute
    
    rows = execute_fn(sql_stmt) if values==None else execute_fn(sql_stmt, values)
    if sql_stmt[:6] in ['DELETE', 'INSERT', 'UPDATE']: return rows
    return self.fetchall()
pymysql.cursors.DictCursor.e = e

def dict_insert(self, data_list:list, table:str):
    """
    Insert a dictionary of data into mysql. On duplicate update. The keys must match the column names

    Parameters
    ==========
    data_list (list(dict)): Must provide a list of dictionaries, where the dict keys match the col names
    table (str): the table to insert the data into

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
    logging.debug(f'dict_insert {len(values)} values: {[v for v in values[:2]]}')
    return self.executemany(sql_string, values)
pymysql.cursors.DictCursor.dict_insert = dict_insert

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

# array_insert tests
#table='TEST_subscriber_data_list'
#data_list=[[803,'', 'test', None, None, None, None, None, None, None, None,None,None,None,None,None],
#     [804,'', 'test', None, None, None, None, None, None, None, None,None,None,None,None,None],
#     [805,'', 'test', None, None, None, None, None, None, None, None,None,None,None,None,None]]
#columns = ['id', 'state', 'first_name', 'email_address', 'created_at', 'fb_audience_source', 'loc_city', 'loc_state', 'loc_lat',
# 'loc_lng', 'lead_opt-in', 'lead_source', 'mailchimp_sub_date', 'stsbc_evg_launch_deadline', 'subscribe_date', 'unsubscribe_date']
#cursor.array_insert(data_list, columns, table)

# Convenience function since I changed the name recently
def print_array(array:list):
    return mysql_array(array)

def mysql_array(array:list):
    if not array: return '(NULL)'
    return '(' + ', '.join([repr(a) for a in array]) + ')'
