#!/usr/bin/env python3
#
# extract bank account information from a DB account
#
# written by: Andreas Scherbaum <andreas@scherbaum.la>
#

import re
import os
import stat
import sys
if sys.version_info[0] < 3:
    reload(sys)
    sys.setdefaultencoding('utf8')
import logging
import tempfile
import argparse
import yaml
import string
import sqlite3
import datetime
import atexit
_urllib_version = False
try:
    import urllib2
    import urllib
    import httplib
    _urllib_version = 2
except ImportError:
    import urllib3
    _urllib_version = 3
    try:
        import httplib
    except ImportError:
        import http.client as httplib
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import gzip
import zlib
from subprocess import Popen
try:
    from urlparse import urljoin # Python2
except ImportError:
    from urllib.parse import urljoin # Python3

import requests
from socket import error as SocketError
import errno

import smtplib
from email.mime.text import MIMEText

#_htmlparser_version = False
#try:
#    import html.parser
#    from html.parser import HTMLParser
#    _htmlparser_version = 3
#except ImportError:
#    import HTMLParser
#    _htmlparser_version = 2
#from html import unescape
import html.parser
from html.parser import HTMLParser
import html as htmlescape


# start with 'info', can be overriden by '-q' later on
logging.basicConfig(level = logging.INFO,
		    format = '%(levelname)s: %(message)s')





#######################################################################
# Config class

class Config:

    def __init__(self):
        self.__cmdline_read = 0
        self.__configfile_read = 0
        self.arguments = False
        self.argument_parser = False
        self.configfile = False
        self.config = False
        self.output_help = True

        if (os.environ.get('HOME') is None):
            logging.error("$HOME is not set!")
            sys.exit(1)
        if (os.path.isdir(os.environ.get('HOME')) is False):
            logging.error("$HOME does not point to a directory!")
            sys.exit(1)



    # config_help()
    #
    # flag if help shall be printed
    #
    # parameter:
    #  - self
    #  - True/False
    # return:
    #  none
    def config_help(self, config):
        if (config is False or config is True):
            self.output_help = config
        else:
            print("")
            print("invalid setting for config_help()")
            sys.exit(1)



    # print_help()
    #
    # print the help
    #
    # parameter:
    #  - self
    # return:
    #  none
    def print_help(self):
        if (self.output_help is True):
            self.argument_parser.print_help()



    # parse_parameters()
    #
    # parse commandline parameters, fill in array with arguments
    #
    # parameter:
    #  - self
    # return:
    #  none
    def parse_parameters(self):
        parser = argparse.ArgumentParser(description = 'Bank account information for DB accounts',
                                         add_help = False)
        self.argument_parser = parser
        parser.add_argument('--help', default = False, dest = 'help', action = 'store_true', help = 'show this help')
        parser.add_argument('-c', '--config', default = '', dest = 'config', help = 'configuration file')
        # store_true: store "True" if specified, otherwise store "False"
        # store_false: store "False" if specified, otherwise store "True"
        parser.add_argument('-v', '--verbose', default = False, dest = 'verbose', action = 'store_true', help = 'be more verbose')
        parser.add_argument('-q', '--quiet', default = False, dest = 'quiet', action = 'store_true', help = 'run quietly')


        # parse parameters
        args = parser.parse_args()

        if (args.help is True):
            self.print_help()
            sys.exit(0)

        if (args.verbose is True and args.quiet is True):
            self.print_help()
            print("")
            print("Error: --verbose and --quiet can't be set at the same time")
            sys.exit(1)

        if not (args.config):
            self.print_help()
            print("")
            print("Error: configfile is required")
            sys.exit(1)

        if (args.verbose is True):
            logging.getLogger().setLevel(logging.DEBUG)

        if (args.quiet is True):
            logging.getLogger().setLevel(logging.ERROR)

        self.__cmdline_read = 1
        self.arguments = args

        return



    # load_config()
    #
    # load configuration file (YAML)
    #
    # parameter:
    #  - self
    # return:
    #  none
    def load_config(self):
        if not (self.arguments.config):
            return

        logging.debug("config file: " + self.arguments.config)

        if (self.arguments.config and os.path.isfile(self.arguments.config) is False):
            self.print_help()
            print("")
            print("Error: --config is not a file")
            sys.exit(1)

        # the config file holds sensitive information, make sure it's not group/world readable
        st = os.stat(self.arguments.config)
        if (st.st_mode & stat.S_IRGRP or st.st_mode & stat.S_IROTH):
            self.print_help()
            print("")
            print("Error: --config must not be group or world readable")
            sys.exit(1)


        try:
            with open(self.arguments.config, 'r') as ymlcfg:
                config_file = yaml.safe_load(ymlcfg)
        except:
            print("")
            print("Error loading config file")
            sys.exit(1)

        # verify all account entries
        errors_in_config = False
        for account in config_file['accounts']:
            for check in ['account_number', 'sub_account', 'branch_code', 'password', 'recipients', 'enabled']:
                try:
                    t = config_file['accounts'][account][check]
                except KeyError:
                    print("")
                    print("Error: missing '" + str(check) + "' in entry: " + str(account))
                    errors_in_config = True
            if (config_file['accounts'][account]['enabled'] != True and config_file['accounts'][account]['enabled'] != False):
                print("")
                print("Error: 'enabled' is invalid, in entry: " + str(account))
                print(config_file['accounts'][account]['enabled'])
                errors_in_config = True

        if (errors_in_config is True):
            sys.exit(1)


        # verify sender address
        try:
            t = config_file['sender_address']
        except KeyError:
            print("")
            print("Error: missing 'sender_address' in config file")


        self.configfile = config_file
        self.__configfile_read = 1

        return


# end Config class
#######################################################################





#######################################################################
# Database class

class Database:

    def __init__(self, config):
        self.config = config

        # database defaults to a hardcoded file
        self.connection = sqlite3.connect(os.path.join(os.environ.get('HOME'), '.db_accounts'))
        self.connection.row_factory = sqlite3.Row
        # debugging
        #self.drop_tables()
        self.init_tables()
        #sys.exit(0);

        atexit.register(self.exit_handler)



    def exit_handler(self):
        self.connection.close()



    # get_account_id()
    #
    # retrieve database ID for account, create if necessary
    #
    # parameter:
    #  - self
    #  - account name (from config file)
    #  - account number
    #  - sub account number
    #  - bank branch code
    # return:
    #  - database ID for account
    def get_account_id(self, account, account_number, sub_account, branch_code):
        logging.debug("Loading account: " + str(account) + " (" + str(branch_code) + "/" + str(account_number) + "/" + str(sub_account) + ")")
        query = """SELECT *
                     FROM bank_accounts
                    WHERE name = ?"""
        result = self.execute_one(query, [account])
        if (result is None):
            # Account does not yet exist in database, create it
            query2 = """INSERT INTO bank_accounts
                                    (name, account_number, sub_account, branch_code)
                             VALUES (?, ?, ?, ?)"""
            self.execute_one(query2, [account, account_number, sub_account, branch_code])
            result = self.execute_one(query, [account])

        # compare the bank account data
        if (result['account_number'] != account_number):
            logging.error("Error: account number for account does not match!")
            sys.exit(1)
        if (result['sub_account'] != sub_account):
            logging.error("Error: sub account for account does not match!")
            sys.exit(1)
        if (result['branch_code'] != branch_code):
            logging.error("Error: branch code for account does not match!")
            sys.exit(1)

        return result['id']



    # init_tables()
    #
    # initialize all missing tables
    #
    # parameter:
    #  - self
    # return:
    #  none
    def init_tables(self):
        if (self.table_exist('bank_accounts') is False):
            logging.debug("need to create table bank_accounts")
            self.table_bank_accounts()

        if (self.table_exist('account_balance') is False):
            logging.debug("need to create table account_balance")
            self.table_account_balance()

        if (self.table_exist('account_statements') is False):
            logging.debug("need to create table account_statements")
            self.table_account_statements()

        if (self.table_exist('user_information') is False):
            logging.debug("need to create table user_information")
            self.table_user_information()



    # drop_tables()
    #
    # drop all existing tables
    #
    # parameter:
    #  - self
    # return:
    #  none
    def drop_tables(self):
        if (self.table_exist('bank_accounts') is True):
            logging.debug("drop table bank_accounts")
            self.drop_table('bank_accounts')

        if (self.table_exist('bank_access_logs') is True):
            logging.debug("drop table bank_access_logs")
            self.drop_table('bank_access_logs')

        if (self.table_exist('account_balance') is True):
            logging.debug("drop table account_balance")
            self.drop_table('account_balance')

        if (self.table_exist('account_statements') is True):
            logging.debug("drop table account_statements")
            self.drop_table('account_statements')

        if (self.table_exist('user_information') is True):
            logging.debug("drop table user_information")
            self.drop_table('user_information')




    # table_exist()
    #
    # verify if a table exists in the database
    #
    # parameter:
    #  - self
    #  - table name
    # return:
    #  - True/False
    def table_exist(self, table):
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute_one(query, [table])
        if (result is None):
            return False
        else:
            return True



    # drop_table()
    #
    # drop a specific table
    #
    # parameter:
    #  - self
    #  - table name
    # return:
    #  none
    def drop_table(self, table):
        # there is no sane way to quote identifiers in Python for SQLite
        # assume that the table name is safe, and that the author of this module
        # never uses funny table names
        query = 'DROP TABLE "%s"' % table
        self.execute_one(query, [])



    # run_query()
    #
    # execute a database query without parameters
    #
    # parameter:
    #  - self
    #  - query
    # return:
    #  none
    def run_query(self, query):
        cur = self.connection.cursor()
        cur.execute(query)
        self.connection.commit()



    # execute_one()
    #
    # execute a database query with parameters, return single result
    #
    # parameter:
    #  - self
    #  - query
    #  - list with parameters
    # return:
    #  - result
    def execute_one(self, query, param):
        cur = self.connection.cursor()

        cur.execute(query, param)
        result = cur.fetchone()

        self.connection.commit()
        return result



    # execute_query()
    #
    # execute a database query with parameters, return result set
    #
    # parameter:
    #  - self
    #  - query
    #  - list with parameters
    # return:
    #  - result set
    def execute_query(self, query, param):
        cur = self.connection.cursor()

        cur.execute(query, param)
        result = cur.fetchall()

        self.connection.commit()
        return result



    # table_bank_accounts()
    #
    # create the 'bank_accounts' table
    #
    # parameter:
    #  - self
    # return:
    #  none
    def table_bank_accounts(self):
        query = """CREATE TABLE bank_accounts (
                id INTEGER PRIMARY KEY NOT NULL,
                added_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                name TEXT NOT NULL UNIQUE,
                account_number BIGINT,
                sub_account INTEGER,
                branch_code INTEGER
                )"""
        self.run_query(query)



    # table_account_balance()
    #
    # create the 'account_balance' table
    #
    # parameter:
    #  - self
    # return:
    #  none
    def table_account_balance(self):
        query = """CREATE TABLE account_balance (
                id INTEGER PRIMARY KEY NOT NULL,
                added_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                bank_account INTEGER NOT NULL,
                account_balance NUMERIC NOT NULL,
                account_balance_currency TEXT NOT NULL,
                FOREIGN KEY (bank_account) REFERENCES bank_accounts(id)
                )"""
        self.run_query(query)



    # table_account_statements()
    #
    # create the 'account_statements' table
    #
    # parameter:
    #  - self
    # return:
    #  none
    def table_account_statements(self):
        query = """CREATE TABLE account_statements (
                id INTEGER PRIMARY KEY NOT NULL,
                added_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                date_of_bookkeeping DATE NOT NULL,
                date_of_value DATE NOT NULL,
                bank_account INTEGER NOT NULL,
                intended_use TEXT NOT NULL,
                intended_use2 TEXT NOT NULL,
                iban TEXT NOT NULL,
                bic TEXT NOT NULL,
                customer_reference TEXT NOT NULL,
                mandate_reference TEXT NOT NULL,
                creditor_id TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                currency TEXT NOT NULL,
                FOREIGN KEY (bank_account) REFERENCES bank_accounts(id)
                )"""
        self.run_query(query)



    # table_user_information()
    #
    # create the 'user_information' table
    #
    # parameter:
    #  - self
    # return:
    #  none
    def table_user_information(self):
        query = """CREATE TABLE user_information (
                id INTEGER PRIMARY KEY NOT NULL,
                added_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                bank_account INTEGER NOT NULL,
                last_seen_statement INTEGER NOT NULL,
                FOREIGN KEY (bank_account) REFERENCES bank_accounts(id),
                FOREIGN KEY (last_seen_statement) REFERENCES account_statements(id)
                )"""
        self.run_query(query)



    # save_account_amount()
    #
    # save current account balance
    #
    # parameter:
    #  - self
    #  - account id
    #  - balance
    #  - currency
    # return:
    #  none
    def save_account_amount(self, account_id, bank_balance, bank_balance_currency):
        query = """INSERT INTO account_balance
                               (bank_account, account_balance, account_balance_currency)
                        VALUES (?, ?, ?)"""
        self.execute_one(query, [account_id, bank_balance, bank_balance_currency])



    # save_account_transactions()
    #
    # save transactions, verify if transactions have been seen before
    #
    # parameter:
    #  - self
    #  - account ID
    #  - list with transactions
    # return:
    #  none
    def save_account_transactions(self, account_id, bookings):
        for transaction in bookings:
            transaction['bank_account'] = account_id
            query = """SELECT *
                         FROM account_statements
                        WHERE bank_account = ?
                          AND date_of_bookkeeping = ?
                          AND date_of_value = ?
                          AND intended_use = ?
                          AND intended_use2 = ?
                          AND iban = ?
                          AND bic = ?
                          AND customer_reference = ?
                          AND mandate_reference = ?
                          AND creditor_id = ?
                          AND amount = ?
                          AND currency= ?"""
            result = self.execute_query(query, [account_id, transaction['date_of_bookkeeping'], transaction['date_of_value'],
                                                transaction['intended_use'], transaction['intended_use2'], transaction['iban'],
                                                transaction['bic'], transaction['customer_reference'], transaction['mandate_reference'],
                                                transaction['creditor_id'], transaction['amount'], transaction['currency']])
            if (len(result) > 1):
                # this is theoretically possible, but in practive more likely an error
                logging.error("Found account booking statement multiple times in the database")
                sys.exit(1)
            if (len(result) == 0):
                query = """INSERT INTO account_statements
                                       (date_of_bookkeeping, date_of_value, bank_account, intended_use,
                                        intended_use2, iban, bic, customer_reference, mandate_reference,
                                        creditor_id, amount, currency)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                self.execute_one(query, [transaction['date_of_bookkeeping'], transaction['date_of_value'], account_id,
                                         transaction['intended_use'], transaction['intended_use2'], transaction['iban'],
                                         transaction['bic'], transaction['customer_reference'], transaction['mandate_reference'],
                                         transaction['creditor_id'], transaction['amount'], transaction['currency']])
                logging.debug("Write booking entry: " + str(transaction['date_of_bookkeeping']) + '/' +
                              str(transaction['date_of_value']) + ': ' + str(transaction['amount']) +
                              ' ' + str(transaction['currency']) + ' (' + str(transaction['intended_use']) + ')')



    # unseen_transactions()
    #
    # return unseen transactions, move "unseen" pointer
    #
    # parameter:
    #  - self
    #  - account ID
    # return:
    #  - list with previously unseen transactions
    def unseen_transactions(self, account_id):
        # first get pointer to last transaction
        query = """SELECT *
                     FROM user_information
                    WHERE bank_account = ?"""
        result = self.execute_one(query, [account_id])
        if (result is None):
            # no previous entry, read all statements and then create an entry
            query = """SELECT *
                         FROM account_statements
                        WHERE bank_account = ?
                     ORDER BY id ASC"""
            result2 = self.execute_query(query, [account_id])
            if (len(result2) > 0):
                query = """INSERT INTO user_information
                                       (bank_account, last_seen_statement)
                                VALUES (?, ?)"""
                self.execute_one(query, [account_id, result2[-1]['id']])
        else:
            # existing previous entry, read only new statements and update the entry
            query = """SELECT *
                         FROM account_statements
                        WHERE bank_account = ?
                          AND id > ?
                     ORDER BY id ASC"""
            result2 = self.execute_query(query, [account_id, result['last_seen_statement']])
            if (len(result2) > 0 and result2[-1]['id'] != result['last_seen_statement']):
                query = """UPDATE user_information
                              SET last_seen_statement = ?
                            WHERE id = ?"""
                self.execute_one(query, [result2[-1]['id'], account_id])

        return result2



    # last_account_balance()
    #
    # return the latest account balance available in the database
    #
    # parameter:
    #  - self
    #  - account ID
    # return:
    #  - last available entry
    def last_account_balance(self, account_id):
        query = """SELECT *
                     FROM account_balance
                    WHERE bank_account = ?
                 ORDER BY id desc
                    LIMIT 1"""

        return self.execute_one(query, [account_id])




# end Database class
#######################################################################



#######################################################################
# functions for the main program





# from: http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
# human_size()
#
# format number into human readable output
#
# parameters:
#  - number
# return:
#  - string with formatted number
def human_size(size_bytes):
    """
    format a size in bytes into a 'human' file size, e.g. bytes, KB, MB, GB, TB, PB
    Note that bytes/KB will be reported in whole numbers but MB and above will have greater precision
    e.g. 1 byte, 43 bytes, 443 KB, 4.3 MB, 4.43 GB, etc
    """
    if (size_bytes == 1):
        # because I really hate unnecessary plurals
        return "1 byte"

    suffixes_table = [('bytes',0),('KB',0),('MB',1),('GB',2),('TB',2), ('PB',2)]

    num = float(size_bytes)
    for suffix, precision in suffixes_table:
        if (num < 1024.0):
            break
        num /= 1024.0

    if (precision == 0):
        formatted_size = "%d" % num
    else:
        formatted_size = str(round(num, ndigits=precision))

    return "%s %s" % (formatted_size, suffix)



# get_url()
#
# GET a specific url, handle compression
#
# parameter:
#  - url
#  - requests object
#  - data (optional, dictionary)
# return:
#  - content of the link
def get_url(url, session, data = None):

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests.packages.urllib3").setLevel(logging.WARNING)
    logging.getLogger("httplib").setLevel(logging.WARNING)
    # set language to 'German', all content will be rendered in German and all functionality is available
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language' : 'de'}

    if (data is None):
        # GET request
        rs = session.request('GET', url, headers = headers)
    else:
        # POST request
        rs = session.request('POST', url, data = data, headers = headers)


    if (rs.status_code != 200):
        if (rs.status_code == 400):
            logging.error("HTTPError = 400 (Bad Request)")
        elif (rs.status_code == 401):
            logging.error("HTTPError = 401 (Unauthorized)")
        elif (rs.status_code == 403):
            logging.error("HTTPError = 403 (Forbidden)")
        elif (rs.status_code == 404):
            logging.error("HTTPError = 404 (URL not found)")
        elif (rs.status_code == 408):
            logging.error("HTTPError = 408 (Request Timeout)")
        elif (rs.status_code == 418):
            logging.error("HTTPError = 418 (I'm a teapot)")
        elif (rs.status_code == 500):
            logging.error("HTTPError = 500 (Internal Server Error)")
        elif (rs.status_code == 502):
            logging.error("HTTPError = 502 (Bad Gateway)")
        elif (rs.status_code == 503):
            logging.error("HTTPError = 503 (Service Unavailable)")
        elif (rs.status_code == 504):
            logging.error("HTTPError = 504 (Gateway Timeout)")
        else:
            logging.error("HTTPError = " + str(rs.status_code) + "")
        sys.exit(1)

    if (len(rs.text) == 0):
        logging.error("failed to download the url")
        sys.exit(1)

    data = rs.text

    logging.debug("fetched " + human_size(len(data)))

    return data



# extract_form_data()
#
# extract fields from a HTML form
#
# parameter:
#  - content of the HTML form
#  - base URL for the website
# return:
#  - dictionary with 'action' as new URL, and 'fields'
def extract_form_data(form_content, base_url):
    data = {}
    data['action'] = None
    data['fields'] = {}

    # first extract the target for the form
    form_action = re.search('<form.+?action="(.+?)".*?>(.*)<\/form>', form_content, re.DOTALL)
    if (form_action):
        # and normalize it
        data['action'] = urljoin(base_url, str(form_action.group(1)))
        form3_inner_content = str(form_action.group(2))
    else:
        # not finding a target is a problem
        logging.error("Can't extract action field from form!")
        sys.exit(1)


    # go through the form, line by line
    for line in form3_inner_content.splitlines(True):
        #print("line: " + line)
        # this assumes that the field is in one line
        line_hidden = re.search('<input type.*?=.*?"hidden".*?name.*?=.*?"(.+?)".*?value.*?="(.*?)"', line)
        if (line_hidden):
            logging.debug("found   hidden: " + str(line_hidden.group(1)) + " = '" + str(line_hidden.group(2)) + "'")
            data['fields'][str(line_hidden.group(1))] = str(line_hidden.group(2))


        # this assumes that the field is in one line
        line_text = re.search('<input type.*?=.*?"text".*?name.*?=.*?"(.+?)".*?value.*?="(.*?)"', line)
        if (line_text):
            logging.debug("found     text: " + str(line_text.group(1)) + " = '" + str(line_text.group(2)) + "'")
            data['fields'][str(line_text.group(1))] = str(line_text.group(2))


        # this assumes that the field is in one line
        line_password = re.search('<input type.*?=.*?"password".*?name.*?=.*?"(.+?)".*?value.*?="(.*?)"', line)
        if (line_password):
            logging.debug("found password: " + str(line_password.group(1)) + " = '" + str(line_password.group(2)) + "'")
            data['fields'][str(line_password.group(1))] = str(line_password.group(2))


        # this deals with multiple lines
        line_select = re.search('<select.*?name.*?=.*?"(.+?)"', line)
        if (line_select):
            l3_select2 = re.search('<select.*?name.*?=.*?"' + str(line_select.group(1)) + '".*?>(.+?)<\/select>', form3_inner_content, re.DOTALL)
            if (l3_select2):
                try:
                    l3_select3 = str(l3_select2.group(1))
                except UnicodeDecodeError:
                    l3_select3 = str(l3_select2.group(1))
            else:
                logging.error("Found select field (" + str(line_select.group(1)) + "), but no option field!")
                sys.exit(1)

            l3_select4 = re.search('<option value="([^"]*?)" selected=', l3_select3, re.DOTALL)
            if (l3_select4):
                # found a select option which is preselected
                data['fields'][str(line_select.group(1))] = str(l3_select4.group(1))
            else:
                l3_select5 = re.search('.*?<option.*?value="(.*?)"', l3_select3, re.DOTALL)
                if (l3_select5):
                    data['fields'][str(line_select.group(1))] = str(l3_select5.group(1))
                else:
                    logging.error("Found select field (" + str(line_select.group(1)) + "), but no option field!")
            logging.debug("found   select: " + str(line_select.group(1)) + " = '" + str(data['fields'][str(line_select.group(1))]) + "'")


        # this deals with multiple lines, but one radio field per line
        line_radio = re.search('<input type.*?=.*?"radio".*?name.*?=.*?"(.+?)".*?value.*?="(.*?)"', line)
        if (line_radio):
            # if it's the 'checked' radio button, overwrite any previous value
            line_radio2 = re.search('<input type.*?=.*?"radio".*?name.*?=.*?"(.+?)".*?value.*?="(.*?)"[^>]+checked=', line)
            if (line_radio2):
                logging.debug("found    radio: " + str(line_radio2.group(1)) + " = '" + str(line_radio2.group(2)) + "'")
                data['fields'][str(line_radio2.group(1))] = str(line_radio2.group(2))
            else:
                try:
                    t = data['fields'][str(line_radio.group(1))]
                except KeyError:
                    # no previous value stored, must be the first radio button
                    data['fields'][str(line_radio.group(1))] = str(line_radio.group(2))
                    logging.debug("found    radio: " + str(line_radio.group(1)) + " = '" + str(line_radio.group(2)) + "'")

    return data



# retrieve_bank_account_data()
#
# log in into the website, and retrieve all bank account data
#
# parameter:
#  - account data
#  - requests handle
def retrieve_bank_account_data(account, session):

    # Note: the following code is not very nice, because it has to deal with multiple requests
    #       (main website, banking website, login, account overview, data extract) and find the
    #       correct links and fields in every page


    #html = HTMLParser.HTMLParser()
    html = HTMLParser()
    # start on the main website, there is a link to the banking website
    url = 'https://www.' + 'deutsche' + '-' + 'bank' + '.de/'


    # fetch main website
    req = get_url(url, session)
    #print(req)
    # problem description:
    # although the regex is made non-greedy, Python still matches from the first <a>
    # "consuming" everything in front with a .* makes the regex run forever
    # split by links, remove newlines, then search for the link
    l_r = re.findall('<a.+?<\/a>', req, re.DOTALL)
    if (l_r):
        l_r = [l.replace("\n", " ") for l in l_r]
        #logging.debug(l_r)
    else:
        logging.error("Can't identify link to Online Banking (1)!")
        sys.exit(1)

    url_banking = None
    for l in l_r:
        #print(l)
        #l_r2 = re.search('.+?href="(https:.+?trxm.*?)".*?title=".*?Online.Banking.*?">.*?Online\-Banking.*?<\/a>', l)
        l_r2 = re.search('.*?title=".*?Online.Banking.*?".+?href="(https:.+?trxm.*?)">.*?Online\-Banking.*?<\/a>', l)
        if (l_r2):
            url_banking = l_r2.group(1)
            logging.debug("next link (2): " + url_banking)
            break

    if (url_banking is None):
        logging.error("Can't identify link to Online Banking (2)!")
        sys.exit(1)

    if (len(url_banking) > 100):
        logging.error("Can't identify link to Online Banking (3)!")
        logging.error("Link too long!")
        sys.exit(1)



    # fetch Online Banking page
    req_banking = get_url(url_banking, session)
    req_banking = remove_cookie_consent_box(req_banking)
    #req_banking = remove_search_box(req_banking)
    #print(req_banking)

    # the result should only have one <form> object
    l_banking_r_forms = re.search('<form.+<form', req_banking, re.DOTALL)
    if (l_banking_r_forms):
        logging.error("Found multiple forms in login page!")
        sys.exit(1)


    l_banking_r_form = re.search('(<form.+?action=".+?".*?>.*<\/form>)', req_banking, re.DOTALL)
    if (l_banking_r_form):
        form_login_content = str(l_banking_r_form.group(1))
    else:
        logging.error("Can't extract form from login page!")
        sys.exit(1)

    #print(form_login_content)
    data_login = extract_form_data(form_login_content, url_banking)
    url_login = data_login['action']

    #print(req_banking)
    logging.debug("next link (3): " + url_login)


    # verify that the form has all fields we need for login
    for check in ['branch', 'account', 'subaccount', 'pin']:
        try:
            t = data_login['fields'][check]
        except KeyError:
            print("")
            print("Error: missing '" + str(check) + "' in login form")
            sys.exit(1)

    # fill in login values
    data_login['fields']['branch'] = account['branch_code']
    data_login['fields']['account'] = account['account_number']
    data_login['fields']['subaccount'] = "%02d" % account['sub_account']
    data_login['fields']['pin'] = account['password']

    #print(data_login)
    #print(form_login_content)
    #sys.exit(0)


    # login into website
    req_login = get_url(url_login, session, data_login['fields'])
    #print(req_login)
    #sys.exit(0)


    # need the link to "Konten"
    url_accounts = False
    for line in form_login_content.splitlines(True):
        l_accounts_r = re.search('<a href="(.+?)".*?>Konten<\/a>', req_login)
        if (l_accounts_r):
            url_accounts = urljoin(url_login, str(l_accounts_r.group(1)))
            break
    if (url_accounts is False):
        print("")
        print("Can't identify link for 'Konten'")
        sys.exit(1)
    logging.debug("next link (4): " + url_accounts)
    req_accounts = get_url(url_accounts, session)


    l_accounts_r_form = re.search('(<form.+?id="accountTurnoversForm".+?action=".+?".*?>.*?<\/form>)', req_accounts, re.DOTALL)
    if (l_accounts_r_form):
        form_accounts_content = str(l_accounts_r_form.group(1))
    else:
        logging.error("Can't extract form from accounts page!")
        sys.exit(1)

    data_accounts = extract_form_data(form_accounts_content, url_accounts)
    url_data = data_accounts['action']

    #print(req2)
    logging.debug("next link (5): " + data_accounts['action'])
    #print(data_accounts['fields'])

    # verify that the form has all fields we need to retrieve account movements
    for check in ['period', 'periodDays']:
        try:
            t = data_accounts['fields'][check]
        except KeyError:
            print("")
            print("Error: missing '" + str(check) + "' in data form")
            sys.exit(1)

    # set required values
    data_accounts['fields']['periodDays'] = '85'
    data_accounts['fields']['period'] = 'fixedRange'
    data_accounts['fields']['subaccountAndCurrency'] = "%02d" % account['sub_account']


    req_data = get_url(url_data, session, data_accounts['fields'])

    #print(req_data)


    # extract the booking data
    account_data = {}
    account_data['bank_balance'] = None
    account_data['bank_balance_currency'] = None
    bookings = re.search('<... Display bookedTurnovers.+?>(.+?)<... If there are no turnovers existent .+? shown above ..>', req_data, re.DOTALL)
    if (bookings):
        try:
            bookings_data = str(bookings.group(1))
        except UnicodeDecodeError:
            bookings_data = str(bookings.group(1))
        bookings = re.search('<tr class="headline">.+?<\/tr>.*?<tr>.+?<\/tr>(.+)$', bookings_data, re.DOTALL)
        if (bookings):
            try:
                bookings_data = str(bookings.group(1))
            except UnicodeDecodeError:
                bookings_data = str(bookings.group(1))
            #print(bookings_data)
        account_data['bookings'] = []
        # here it get's complicated: the output can have rows and tables stacked
        # there is no clear split pattern
        # go through the data line by line, and search for 'headers="bTentry"' as pattern for a new entry
        # don't forget the last booking before finishing the data
        date_of_bookkeeping = None
        date_of_value = None
        intended_use = None
        intended_use2 = ''
        iban = ''
        bic = ''
        customer_reference = ''
        mandate_reference = ''
        creditor_id = ''
        amount = None
        currency = None


        # debit and credit amount is split across several lines
        # and "disturbed" by the "Lastschriftrueckgabe" (returning a direct debit) link
        bookings_data = re.sub('<a href=.+?>Lastschrift.+?<\/a>', '', bookings_data)
        bookings_data = re.sub('(<td.*?>)\s*', '\g<1>', bookings_data, flags = re.DOTALL | re.MULTILINE)
        bookings_data = re.sub('\s*(<\/td>)', '\g<1>', bookings_data, flags = re.DOTALL | re.MULTILINE)
        #bookings_data = re.sub('(<\/a>)\s*([0-9\-\.,]+)', '\g<1>\g<2>', bookings_data, flags = re.DOTALL | re.MULTILINE)


        for line in bookings_data.splitlines(True):
            btentry = re.search('<td headers="bTentry".*?>([0-9\.]+)</td>', line)
            if (btentry):
                # first write entry with existing data
                if (date_of_bookkeeping is not None):
                    t = {}
                    t['date_of_bookkeeping'] = date_of_bookkeeping
                    t['date_of_value'] = date_of_value
                    t['intended_use'] = intended_use
                    t['intended_use2'] = intended_use2
                    t['iban'] = iban
                    t['bic'] = bic
                    t['customer_reference'] = customer_reference
                    t['mandate_reference'] = mandate_reference
                    t['creditor_id'] = creditor_id
                    t['amount'] = amount
                    t['currency'] = currency
                    if (amount is None or currency is None):
                        logging.error("Could not extract currency or amount!")
                        sys.exit(1)
                    account_data['bookings'].append(t)
                    #print("date_of_bookkeeping: " + str(date_of_bookkeeping))
                    #print("date_of_value: " + str(date_of_value))
                    #print("intended_use: " + str(intended_use))
                    #print("intended_use2: " + str(intended_use2))
                    #print("iban: " + str(iban))
                    #print("bic: " + str(bic))
                    #print("amount: " + str(amount))
                    #print("currency: " + str(currency))
                    #print("customer_reference: " + str(customer_reference))
                    #print("mandate_reference: " + str(mandate_reference))
                    #print("creditor_id: " + str(creditor_id))
                    #print("")
                    #print("")
                    #print("")
                    logging.debug("Found booking entry: " + str(date_of_bookkeeping) + '/' + str(date_of_value) + ': ' + str(amount) + ' ' + str(currency) + ' (' + str(intended_use) + ')')
                    #sys.exit(0)
                # then reset everything
                date_of_bookkeeping = None
                date_of_value = None
                intended_use = None
                intended_use2 = ''
                iban = ''
                bic = ''
                customer_reference = ''
                mandate_reference = ''
                creditor_id = ''
                amount = None
                currency = None
                # now get the date
                date_of_bookkeeping = btentry.group(1)

            btvalue = re.search('<td headers="bTvalue".*?>([0-9\.]+)</td>', line)
            if (btvalue):
                date_of_value = btvalue.group(1)

            btpurpose = re.search('<td headers="bTpurpose".*?>(.*?)</td>', line, re.DOTALL)
            if (btpurpose):
                intended_use = htmlescape.unescape(btpurpose.group(1).strip())

            btdebit = re.search('<td headers="bTdebit".*?>\s*([0-9\.\-,]+)\s*</td>', line)
            if (btdebit):
                amount = fix_punctation(btdebit.group(1))

            btcredit = re.search('<td headers="bTcredit".*?>\s*([0-9\.\-,]+)\s*</td>', line)
            if (btcredit):
                amount = btcredit.group(1)

            btcurrency = re.search('<td headers="bTcurrency".*?>(.*?)</td>', line)
            if (btcurrency):
                currency = btcurrency.group(1).strip()

            btintended_use2 = re.search('<td.*?>Verwendungszweck</td><td.*?>(.+?)<\/td>', line)
            if (btintended_use2):
                intended_use2 = htmlescape.unescape(btintended_use2.group(1).strip())

            btiban = re.search('<td.*?>IBAN</td><td.*?>(.+?)<\/td>', line)
            if (btiban):
                iban = btiban.group(1).strip()

            btbic = re.search('<td.*?>BIC</td><td.*?>(.+?)<\/td>', line)
            if (btbic):
                bic = btbic.group(1).strip()

            btcustomer_reference = re.search('<td.*?>Kundenreferenz</td><td.*?>(.+?)<\/td>', line)
            if (btcustomer_reference):
                customer_reference = btcustomer_reference.group(1).strip()

            btmandate_reference = re.search('<td.*?>Mandatsreferenz</td><td.*?>(.+?)<\/td>', line)
            if (btmandate_reference):
                mandate_reference = btmandate_reference.group(1).strip()

            btcreditor_id = re.search('<td.*?>Gl.*?ubiger ID</td><td.*?>(.+?)<\/td>', line)
            if (btcreditor_id):
                creditor_id = btcreditor_id.group(1).strip()

    else:
        logging.error("")
        logging.error("Missing bookings in retrieved data")
        sys.exit(1)


    current_amount = re.search('>Aktueller Kontostand<.+?class="balance credit"><strong>\s*([0-9,\.\-]+)\s*<\/strong>', req_data, re.DOTALL | re.MULTILINE)
    if (current_amount):
        #print(fix_punctation(current_amount.group(1)))
        account_data['bank_balance'] = str(fix_punctation(current_amount.group(1)))
    else:
        logging.error("")
        logging.error("Missing current amount in retrieved data")
        sys.exit(1)


    current_amount_currency = re.search('>Aktueller Kontostand<.+?class="balance credit">.+?<\/strong>.+?<strong.*?><acronym.*?>(.+?)<\/acronym', req_data, re.DOTALL | re.MULTILINE)
    if (current_amount_currency):
        account_data['bank_balance_currency'] = str(current_amount_currency.group(1))
    else:
        logging.error("")
        logging.error("Missing current amount currency in retrieved data")
        sys.exit(1)


    if (account_data['bank_balance'] is None or account_data['bank_balance_currency'] is None):
        logging.error("Could not extract current balance or currency")
        sys.exit(1)

    logging.debug("Current account balance: " + str(account_data['bank_balance']) + " " + str(account_data['bank_balance_currency']))

    #sys.exit(0)
    return account_data



# fix_punctation()
#
# fix the punctation for money values
#
# parameter:
#  - amount
# return:
#  - amount
def fix_punctation(amount):
    # the german output splits the thousands by a dot, and the cents by a comma
    return amount.replace('.', '').replace(',', '.')



# remove_cookie_consent_box()
#
# remove div and form with cookie consent box
#
# parameter:
#  - HTML content
# return:
#  - HTML content
def remove_cookie_consent_box(content):
    content = re.sub(r'<div id="cookieConsentBox">.+?<form.*?</form>.*?</div>.*?</div>', '', content, flags=re.DOTALL)

    return content



#######################################################################
# main program

config = Config()
config.parse_parameters()
config.load_config()

database = Database(config)

logging.debug("urllib version: " + str(_urllib_version))
# loop over the accounts in the config file
for account in config.configfile['accounts']:
    if (config.configfile['accounts'][account]['enabled'] != True):
        logging.debug("Account '" + str(account) + "' is disabled in config")
        continue

    logging.info("Account: " + str(account))
    logging.debug("Information recipient: " + str(config.configfile['accounts'][account]['recipients']))
    account_id = database.get_account_id(account,
                                         config.configfile['accounts'][account]['account_number'],
                                         config.configfile['accounts'][account]['sub_account'],
                                         config.configfile['accounts'][account]['branch_code'])
    logging.debug("Database id for account is: " + str(account_id))
    session = requests.session()
    account_data = retrieve_bank_account_data(config.configfile['accounts'][account], session)

    database.save_account_amount(account_id, account_data['bank_balance'], account_data['bank_balance_currency'])
    database.save_account_transactions(account_id, account_data['bookings'])
    message = '' + "\n"
    message += '' + "\n"
    last_account_balance = database.last_account_balance(account_id)
    if (len(last_account_balance) == 0):
        # no data at all
        continue
    message += 'Datum: ' + last_account_balance['added_ts'] + "\n"
    message += 'Kontostand: ' + str(last_account_balance['account_balance']) + ' ' + last_account_balance['account_balance_currency'] + "\n"
    message += '' + "\n"
    message += '' + "\n"

    unseen_data = database.unseen_transactions(account_id)
    for line in unseen_data:
        message += '            Betrag: ' + str(line['amount']) + ' ' + str(line['currency']) + "\n"
        message += '     Buchungsdatum: ' + str(line['date_of_bookkeeping']) + "\n"
        message += 'Wertstellungsdatum: ' + str(line['date_of_value']) + "\n"
        message += '  Verwendungszweck: ' + str(line['intended_use']) + "\n"
        if (len(line['intended_use2']) > 0):
            message += '  Verwendungszweck: ' + str(line['intended_use2']) + "\n"
        message += '' + "\n"
        message += '' + "\n"

    try:
        email = smtplib.SMTP('localhost')
        msg = MIMEText(message, 'plain', 'utf8')
        #msg.set_charset('utf8')
        msg['Subject'] = 'Konto Informationen: %s (%s/%s/%s)' % (str(account),
                                                                 str(config.configfile['accounts'][account]['branch_code']),
                                                                 str(config.configfile['accounts'][account]['account_number']),
                                                                 str(config.configfile['accounts'][account]['sub_account']))
        msg['To'] = str(config.configfile['accounts'][account]['recipients'])
        msg['From'] = str(config.configfile['sender_address'])
        email.sendmail(str(config.configfile['sender_address']), str(config.configfile['accounts'][account]['recipients']).split(','), msg.as_string())
        email.quit()
    except smtplib.SMTPServerDisconnected:
        logging.error("Unable to send email!")
        sys.exit(1)
    #print(message)

