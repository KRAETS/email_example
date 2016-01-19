#!/usr/bin/env python
# 
# Logic for parsing the Enron Email Corpus Database
# and loading it into MongoDB.
#
# This script is based on work by Bryan Nehl (http://soloso.blogspot.com/2011/07/getting-enron-mail-database-into.html)
# 
# The Enron source data is available at http://www.cs.cmu.edu/~enron/

import datetime
from email.parser import Parser
import email.utils
import filecmp
import getopt
import hashlib
import os
import random
import re
import sys
import time

import dateutil.parser
from pymongo import Connection
from pytz import timezone
import pytz


MAX_RUN_SIZE = 1
counter = 1
p = Parser()

def remove_forwarding(contents):
    '''Removes sections that contain forwards.  Assumes that forwards are the last part of the message'''
    msg = None
    try:
        msg = p.parsestr(contents.encode("utf-8"))

    except Exception, e:
        return msg
    #print "Message UNTOUCHED",msg
    msgbody = msg._payload
#     print "Matching forwards..."
    #print "Message:", msgbody
    result = re.sub(r'-{4,}.+-{4,}.*$','',msgbody,flags=re.DOTALL)
    #print "Result", result
    if result == msgbody:
        pass
#         print "No match..."
    else:
        pass
#         print "Match!"
    msg._payload = result
    return str(msg._payload)

def remove_special_characters(contents):
    '''Removes all non alpha numeric charactes from a string'''
#     print "Checking contents:"
#     print contents
    msg = None
    try:
        msg = p.parsestr(contents.encode("utf-8"))

    except Exception, e:
        return msg
    
    msgbody = msg._payload
#     print "Matching special characters..."
    #print "Before:", msgbody
    result = re.sub(r'[^A-Za-z0-9]+', ' ', msgbody)
    #print "After", result
    if result == msgbody:
        pass
#         print "No match..."
    else:
        pass
#         print "Match!"
    msg._payload = result;
    return str(msg._payload)

def pre_process(content):
    result = content
    try:
        result = remove_forwarding(result)
        result = remove_special_characters(result)
    except Exception as e:
        print e
    return result

def getFileContents(nameOfFileToOpen):
    dataFile = open(nameOfFileToOpen)
    contents = ""
    try:
        for dataLine in dataFile:
            contents += dataLine

    finally:
        dataFile.close()
    return contents.decode('cp1252')

def saveToDatabase(mailboxOwner, subFolder, filename, contents):
    msg = p.parsestr(contents.encode("utf-8"))
    #ORIGINAL INSERTION FORMAT:
#     document = {"mailbox" : mailboxOwner,
#                 "subFolder" : subFolder,
#                 "filename" : filename,
#                 "headers": dict( msg._headers ),
#                 "body": msg._payload,
#                 "dataUsage": "None"
#                 }
    headersdict = dict(msg._headers)
    document = {"sent_time": headersdict["Date"],
                "message_folder":subFolder,
                "message_body": msg._payload,
                "message_mailbox":mailboxOwner,
                "recipient_address":headersdict["To"],
                "message_id":headersdict["Message-ID"],
                "email_attachment":None,
                "received_time":headersdict["Date"],
                "sender_address":headersdict["From"],
                "message_subject":headersdict["Subject"],
                "recipient_count":len(headersdict["To"].split(','))
                }
#Code to save date as date objects
    try:
        #datetime_obj = datetime.datetime.fromtimestamp(time.mktime(email.utils.parsedate(document["headers"]["Date"])))
        datetime_obj = datetime.datetime.fromtimestamp(time.mktime(email.utils.parsedate(document["received_time"])))
 
        TZINFOS = {
            'PDT': timezone('US/Pacific'),
            # ... add more to handle other timezones
            # (I wish pytz had a list of common abbreviations)
        }
#Original code         
#         datestring = document["headers"]["Date"]
        datestring = document["received_time"]
        
        
         
        TZOFFSETS = {"PDT": -25200}#-7:00
         
        # Parse the string using dateutil
        datetime_in_pdt = dateutil.parser.parse(datestring, tzinfos= TZOFFSETS)
        dtstring = str(datetime_in_pdt)
        
        
        #Check for time
        #TODO
        
        if not (dtstring >= '2000-01-01 00:00:00-07:00' and dtstring < '2000-05-31 23:59:59-07:00'):
#             print 'Skipping because of date'
            return
            
#         datetime_in_utc = datetime_in_pdt.astimezone(pytz.utc)
#         datetime_in_pdt = datetime_in_utc.astimezone(pytz.timezone('US/Pacific'))

#Original code
# #         document["headers"]["Date"] = datetime_in_pdt

        document["received_time"] = dtstring
        document["sent_time"] = dtstring
    except Exception,e:
        raise(e)

#Original code
#     document['body'] = pre_process(msg._payload)
#     if document['body'].strip()=="":
#         raise ValueError("Empty Body, not inserting")

    document['message_body'] = pre_process(msg._payload)
    
    if document['message_body'].strip()=="":
        raise ValueError("Empty Body, not inserting")
    if not (document['message_folder'] == "sent" or document['message_folder']=='sent_items'):
#         print 'skipping because of folder'
        return

    
    messages = db.email_message_table
    messages.insert(document)
    return

if __name__ == "__main__":

    if len(sys.argv) < 2:
        raise Exception("Please specify the path to the enron data.")
    else:
        MAIL_DIR_PATH = sys.argv[1]
        if not os.path.isdir(MAIL_DIR_PATH):
            raise Exception("Invalid or not found path for Enron Input: %s" % MAIL_DIR_PATH)

    PREFIX_TRIM_AMOUNT = len(MAIL_DIR_PATH) + 1
    cn = Connection('localhost')
    db = cn.enron_mail_clean
#     print "database initialized {0}".format(datetime.datetime.now())

    for root, dirs, files in os.walk(MAIL_DIR_PATH,topdown=False):
        directory = root[PREFIX_TRIM_AMOUNT:]

        # extract mail box owner
        parts = directory.split('/')
        if len(parts)<=1:
            continue
        mailboxOwner = parts[1]

        # sub-folder info
        if 3 == len(parts):
            subFolder = parts[2]
        else:
            subFolder = ''

        # distinct file name
        for filename in files:

            # get the file contents
            nameOfFileToOpen = "{0}/{1}".format(root,filename)
            try:
                contents = getFileContents(nameOfFileToOpen)
                saveToDatabase(mailboxOwner, subFolder, filename, contents)
            except Exception, e:
                pass
#                 print "Possible error with charset", e

            counter += 1
            if counter % 1000 == 0:
                print("{0} {1}".format(counter,datetime.datetime.now()))

#     print "database closed {0}".format(datetime.datetime.now())
#     print "{0} total records processed".format(counter - 1)
