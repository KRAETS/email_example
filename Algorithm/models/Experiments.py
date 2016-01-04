"""
Created on Mar 6, 2015

@author: pe25171
"""

import ConfigParser
import datetime
from email.parser import Parser
import email.utils
import math
import os
from random import shuffle
import re
import shutil
import subprocess
import sys
import time

from dateutil.parser import parse
import dateutil.parser
from pymongo import MongoClient
import pymongo
from pytz import timezone
import pytz


def get_distinct_users():
    '''
    Finds all of the distinct users in the database
    and saves them to a file
    Goes through the names of the inboxes and uses those as unique company users
    '''
    f = open('myfile','r+')
    list = []
    content = f.readlines()
    #If empty file, recreate it
    if len(content)==0:
        print "Starting to parse users..."
        client = MongoClient("localhost", 27017)
        db = client.enron_mail
        messages = db.messages
        counter = 0
        for message in messages.find():
            print "Getting user",counter
            #print "Message",message
            if message["mailbox"] in list:
                continue
            else:
                list.append(message["mailbox"])
            counter=counter+1
        list.sort()
        print "Writing list to file", list
        for item in list:
            f.write(item+'\n')
        client.close()
    else:
        list = content
        list = map(lambda s: s.strip(), list)
    f.close() # you can omit in most cases as the destructor will call if
    return list

def getTimeSortedMessageList():
    '''
    Queries all messages and returns a tuple list which has: 
    (a,b,c,d)
    a=date of original sent/received message
    b=people to whom the messace was sent
    c=sender
    d=folder in their inbox
    '''
    timesortedlist = []
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    messagesum = []
    #Check in specific folders
    for item in messages.find({"$or":[{"subFolder":"sent_items"},{"subFolder":"sent"}]}):
        datestring = item["headers"]["Date"]
        a = parse(datestring)
        a = make_tz_aware(a)
        if a.tzinfo is None:
            pass
        b = []
        c = []
        d = "None"
        try:
            for cc in item["headers"]["To"].split(','): 
                b.append(cc)
        except Exception as e:
            pass#print "No TO field", e
        try:
            for cc in item["headers"]["Cc"].split(','): 
                b.append(cc)
        except Exception as e:
            pass#print "Item has no cc", e
        try:
            c.append(item["headers"]["From"])
        except Exception as e:
            pass#print "Item has no from",e
        try:
            d = item["subFolder"]
        except Exception as e:
            pass#print "Item has no subfolder
        #print a,b,c
        itemtuple = (a,b,c,d)
        messagesum.append(itemtuple)
    client.close()
    messagesum.sort(key=lambda tup: tup[0]) 
    timesortedlist = messagesum
    return timesortedlist

def make_tz_aware(dt):
    """Add timezone information to a datetime object, only if it is naive."""
    tz =timezone("America/Los_Angeles")
    try:
        dt = tz.localize(dt)
    except Exception as e:
        pass
    return dt

def find_and_print_intervals(startdate, enddate, tuplelist):
    daydelta = 0
    minutedelta = 1*60*60
    startinterval = startdate
    finishinterval = startdate + datetime.timedelta(daydelta,minutedelta)
    intervaltuplelist = []
    intervalnum = 0
    #Repeat until we hit the end
    while True:
        #Each rep is an interval
        interval = []
        
        for tuple in tuplelist:
            #Assuming the list is ordered we can just break
            if make_tz_aware(parse(tuple.split(';')[0]))>finishinterval:
                break
            #Exact interval start
            if make_tz_aware(parse(tuple.split(';')[0])) == startinterval:
                print "Added"
                interval.append(tuple)
            #Exact finish time
            elif make_tz_aware(parse(tuple.split(';')[0])) == finishinterval:
                print "Added"
                interval.append(tuple)
            #In interval
            elif make_tz_aware(parse(tuple.split(';')[0]))>startinterval and make_tz_aware(parse(tuple.split(';')[0]))<finishinterval:
                print "Added"
                interval.append(tuple)
        #Append finds
        if not (len(interval) == 0):
            intervaltuplelist.append((startinterval,finishinterval,interval))
        #Check exit condition
        if(finishinterval>enddate):
            break
        #Shift interval
        else:
            startinterval = startinterval+datetime.timedelta(daydelta,minutedelta)
            finishinterval = finishinterval + datetime.timedelta(daydelta,minutedelta)
#         print finishinterval
    print intervaltuplelist
    f = open("intervaltuplelist","w")

    for tuple in intervaltuplelist:
        for item in tuple:
            f.write(str(item)+";")
        f.write("\n")
    f.close()

def find_domains():
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    domainlist = []
    for message in messages.find():
        #WIthing a message
        #Search in the to:
        try:
            for email in message["headers"]["To"].split(","):
                domain  = re.search("@[\w.]+", email)
                if not (domain.group() in domainlist):
                    domainlist.append(domain.group())
        except Exception as e:
            pass        
        try:
            for email in message["headers"]["From"].split(","):
                domain  = re.search("@[\w.]+", email)
                if not (domain.group() in domainlist):
                    domainlist.append(domain.group())
        except Exception as e:
            pass
            #print "No to", message
    f = open("domainlist","w")
    f.writelines(domainlist)
    f.close()
    client.close()

def domain_communication_frequency():
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    domaindict = {}
    enrondomains = set()
    msglist2 = []
    msglist = db.messages.find({"$or":[{"subFolder":"sent"},{"subFolder":"sent_items"}]})
    for messg in msglist:
        msglist2.append(messg)
        
    for message in msglist2:
        #Within a message
        #Search in the to:
        dlist1 = []
        dlist2 = []
        try:
            for email in message["headers"]["To"].split(","):
                #Get email domain
                domain  = re.search("@[\w.]+", email)
                domainname = domain.group()
                domainname = str(domainname).replace("@", "")
                domainnameparts = domainname.split(".")
                if len(domainnameparts)>2:
                    domainname=domainnameparts[len(domainnameparts)-2]+'.'+domainnameparts[len(domainnameparts)-1]
                dlist1.append(domainname)
        except Exception as e:
            print "Exception in to:",str(e)
        
        totaldomains=dlist1+dlist2
        totaldomains = list(set(totaldomains))
        foundenron = False
        for domain in totaldomains:
                #Find out any other enron domains
                if "enron" in domain:
                    enrondomains.add(domain)
                    if not foundenron:
                        #Not duplicate enron emails.
                        foundenron = True
                        try:
                            domaindict["enron"]=domaindict["enron"]+1
                        except Exception as nokeyexception:
                            print "Adding key", domain
                            domaindict["enron"] = 1
                else:
                    try:
                        domaindict[domain] = domaindict[domain]+1
                    except Exception as nokeyexception:
                        print "Adding key", domain
                        domaindict[domain] = 1
                
            
        
    #Print out the domains that were sent emails
    f = open("addresses_under_domain_freq","w")
    for w in sorted(domaindict, key=domaindict.get, reverse=True):
        f.write(w+":"+str(domaindict[w])+";\n") 
    f.close()
    #Print enron branches
    f = open("enron_domains_freq","w")
    for item in enrondomains:
        f.write(str(item)+"\n")
    f.close()
    #
    
    
    return domaindict

statdict = {}
count   = 0
def domain_statistics(inputfile):
    f = open(inputfile,'r')
    global count, statdict
    count +=1
    
    for line in f.readlines():
        company = re.search(r"\b.*:", line).group(0).strip().replace(":", "")
#         company = company.group(0)
#         company = company.strip()
#         company = company.replace(":", "")
        amount = re.search(r":\d*;", line).group(0).strip().replace(":", "").replace(";","")
        
        try:
            #add the count for each company
            statdict[company]['amount']+=int(amount)
        except Exception,e:
            statdict[company] = {'amount':0,'average':0}
            statdict[company]['amount'] = int(amount)
        statdict[company]['average']=float(statdict[company]['amount']/float(count))
    
def domain_communication_frequency_by_time(From = None, To = None, biweek=None, filename=None):
    
    #Convert into timezones:
    from_in_pdt = None 
    to_in_pdt = None
    
    try:
        TZOFFSETS = {"PDT": -25200}#-7:00
        # Parse the string using dateutil
        from_in_pdt = dateutil.parser.parse(From, tzinfos= TZOFFSETS)
        if biweek is not None:
            to_in_pdt = from_in_pdt + datetime.timedelta(14,0) # days, seconds, then other fields.
        else:
            to_in_pdt = dateutil.parser.parse(To, tzinfos= TZOFFSETS)
        print from_in_pdt,to_in_pdt
    except Exception,e:
        print "Problem with time",e
        raise(e)
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    domaindict = {}
    enrondomains = set()
    enrondomainsammounts = {}
    msglist2 = []
    msglist = db.messages.find({"$and":[{"$or":[
                                                {"subFolder":"sent"},
                                                {"subFolder":"sent_items"}
                                                ]},
                                        {'headers.Date': {
                                            '$gte': from_in_pdt,
                                            '$lt': to_in_pdt 
                                        }}
                                        ]}
                               )
    
    for messg in msglist:
        msglist2.append(messg)
    print "Ammount of messages =",len(msglist2)    
    count = 0
    for message in msglist2:
        #Within a message
        #Search in the to:
        dlist1 = []
        dlist2 = []
        try:
            for email in message["headers"]["To"].split(","):
                #Get email domain
                domain  = re.search("@[\w.]+", email)
                domainname = domain.group()
                domainname = str(domainname).replace("@", "")
                domainnameparts = domainname.split(".")
                
                #Dont touch enron domains
                if len(domainnameparts)>2 :
                    found = False
                    for part in domainnameparts:
                        if "enron" in part:
                            found = True
                            break
                    if not found:
                        domainname=domainnameparts[len(domainnameparts)-2]+'.'+domainnameparts[len(domainnameparts)-1]
                dlist1.append(domainname)
        except Exception as e:
            print "Exception in to:",str(e)
        
        totaldomains=dlist1+dlist2
        #clear duplicates
        totaldomains = list(set(totaldomains))
        foundenron = False
        for domain in totaldomains:
            #Find out any other enron domains
            if "enron" in domain:
                enrondomains.add(domain)
                try:
                    enrondomainsammounts[domain] += 1
                except Exception,e:
                    enrondomainsammounts[domain] = 1
                if not foundenron:
                    #Dont count more than once
#                     count += 1
#                     print "Original enron email",count
#                     print message["subFolder"],message["mailbox"], message["filename"]
                    foundenron = True
                    try:
                        domaindict["enron"]=domaindict["enron"]+1
                    except Exception as nokeyexception:
                        print "Adding key", domain
                        domaindict["enron"] = 1
            else:
                try:
                    domaindict[domain] = domaindict[domain]+1
                except Exception as nokeyexception:
                    print "Adding key", domain
                    domaindict[domain] = 1
                
            
        
    #Print out the domains that were sent emails
    try:
        os.makedirs('output')
    except Exception ,e:
        print e
    
    original_filename = str(filename)
    if filename is not None:
        filename = "output/amount_under_domain_week_"+original_filename
    else:
        filename = "output/addresses_under_domain_all"
    f = open(filename,"w")
    for w in sorted(domaindict, key=domaindict.get, reverse=True):
        f.write(w+":"+str(domaindict[w])+";\n") 
    f.close()
    #Print enron branches
    if filename is not None:
        filename = "output/enron_domains_"+original_filename
    else:
        filename = "output/enron_domains_all"
    f = open(filename,"w")
    for item in enrondomains:
        f.write(str(item)+"\n")
    f.close()    
    #Print enron branch frequencies:
    if filename is not None:
        filename = "output/enron_domains_freq_"+original_filename
    else:
        filename = "output/enron_domains_freq_all"
    f = open(filename,"w")
    for key,item in enrondomainsammounts.iteritems():
        f.write(str(key)+":"+str(item)+"\n")
    f.close()    
    print "Done"
    print msglist
    
def list_domains():
    f = open("domainlist","r")
    longline = f.readline()
    longline = longline.split("@")
    f.close()
    
    f = open("domainlist","w")
    for line in longline:
        f.write(line+"\n")
    f.close()

def group_into_domains():
    f = open("domainlist","r")
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    domain_address_tuple_list = []
    # For all domains
    messagelist = messages.find()
    domaindictionary = {}
    domainlines = f.readlines()
    f.close()
    for domain in domainlines:
        domain = domain.strip()
        domaindictionary[domain] = []
    for domain in domainlines: 
    #Search each message
        domain = domain.strip()
        print "Starting domain:",domain
        msgcount = 1
        for message in messagelist:
            #print "Message",msgcount
            try:
                #For emails in that domain
                for email in message["headers"]["To"].split(","):
                    #Check if the email is in the domain
                    if email.find(domain) == -1:
                        pass
                    #if matching domain
                    else:
                        #Check if email is inserted if not add it
                        if not (email in domaindictionary[domain]):
                            domaindictionary[domain].append(email)
            except Exception as e:
                pass        
            try:
                for email in message["headers"]["From"].split(","):
                    if email.find(domain) == -1:
                        pass
                    #if matching domain
                    else:
                        #Check if email is inserted if not add it
                        if not (email in domaindictionary[domain]):
                            domaindictionary[domain].append(email)
            except Exception as e:
                pass
            msgcount= msgcount+1
    #add the domain and all the addresses
    
    f = open("addresses_under_domain","w")
    for key, value in domaindictionary.iteritems():
        #Write domain name
        f.write(key+":"+str(value)+";\n")
    f.close()

def search_db_ret_list(query_object = None):
    if query_object is None:
        raise("No query provided")
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    domaindict = {}
    enrondomains = set()
    enrondomainsammounts = {}
    msglist2 = []
    msglist = db.messages.find(query_object)
    for messg in msglist:
        msglist2.append(messg)
    client.close()
    return msglist2

def group_companies_by_topics(from_in_pdt, to_in_pdt, malletdir=None, companies_file=None):
    if companies_file is None:
        raise("No configuration file")
    if malletdir is None:
        raise("No mallet directory specified")
    company_dict = {}
    companies_file = open(companies_file,'r')
    for company in companies_file.readlines():
        line = company
        company = re.search(r"\b.*:", line).group(0).strip().replace(":", "")
        amount = float(re.search(r":.*;", line).group(0).strip().replace(":", "").replace(";",""))
        company_dict[company] = amount
    #Companies loaded into dictionary now lets see what these companies are about. Lets find their topics
    for companydomain, value in company_dict.iteritems():
        regx = re.compile(companydomain, re.IGNORECASE)
        messages = search_db_ret_list(
                                      {"$and":[
                                        {"$or":[
                                                    {"subFolder":"sent"},
                                                    {"subFolder":"sent_items"}
                                                ]
                                        },
                                        {'headers.Date': {
                                                '$gte': from_in_pdt,
                                                '$lt': to_in_pdt 
                                        }},
                                        {
                                            'headers.To': regx
                                        }
                                    ]})
        
        
        
        #Train topics on these emails.  Make directories to keep order
        topicsfolder = "topicworking"
        companyrootfolder = topicsfolder+"/"+companydomain.replace(".","ddott")
        if not os.path.exists(companyrootfolder):
            print "Making directories"
            os.makedirs(companyrootfolder)
            os.makedirs(companyrootfolder+"/emails")
            os.makedirs(companyrootfolder+"/emails_development")
            os.makedirs(companyrootfolder+"/topicdata")
        
        #Write them to disk
        counter = 0
        for message in messages:
            f = open(companyrootfolder+"/emails/"+str(counter)+".txt","w")
            f.write(message["body"].strip("\n"))
            f.close()    
            counter+=1
        filelist = []
        for root, dirs, files in os.walk(companyrootfolder+"/emails/",topdown=False):
            for filename in files:
                filelist.append(root+filename)
        shuffle(filelist)
        total = len(filelist)
        percentagetosave = .2
        filestomove = int(math.ceil(total*percentagetosave))
        for filenum in range(filestomove):
            shutil.move(filelist[filenum],companyrootfolder+"/emails_development")
        
        #Convert data to a mallet usable format
        #./bin/mallet import-dir --input enron-data/allen-p-sent_items-datafile
        # --output enron-data/newtextonly/allenPSentItems.mallet --keep-sequence
        # --keep-sequence
        subprocess.call([malletdir+"/bin/mallet", 'import-dir', 
                         '--input', companyrootfolder+"/emails", 
                         '--output', companyrootfolder+"/topicdata/"+companydomain+'.mallet', 
                         '--keep-sequence', 
                         '--remove-stopwords'])
        #Train and classify
        #bin/mallet train-topics --input
#         enron-data/newtextonly/allenPSentItems.mallet --num-topics 20
#         --output-state enron-data/newtextonly/allenp_topic-state.gz
#         --output-topic-keys enron-data/newtextonly/allenp_keys.txt
#         --output-doc-topics enron-data/newtextonly/allenp_composition.txt
        subprocess.call([malletdir+"/bin/mallet", 'train-topics', 
                         '--input', companyrootfolder+"/topicdata/"+companydomain+'.mallet',
                         '--num-topics', '10',
                         '--output-state', companyrootfolder+'/topicdata/'+companydomain.replace(".","_")+'_topic-state.gz',
                         '--output-topic-keys', companyrootfolder+'/topicdata/'+companydomain.replace(".","_")+'_keys.txt',
                         '--output-doc-topics', companyrootfolder+'/topicdata/'+companydomain.replace(".","_")+'_composition.txt'])
        print "Done"
def main():
    config = ConfigParser.RawConfigParser()
    config.read('commext.cfg')
#Configuration file generator for examples
#     config.add_section('Community Extractor')
#     config.set('Community Extractor', 'malletdir', '/Users/pe25171/Downloads/mallet-2.0.7')
#     config.set('Community Extractor', 'fromdate', '1 Jan 2001 0:00:00 -0700 (PDT)')
#     config.set('Community Extractor', 'todate', '1 Jan 2002 0:00:00 -0700 (PDT)')
#     config.set('Community Extractor', 'domain_email_count_filename', 'amount_under_domain_week_')
#     config.set('Community Extractor', 'enron_sub_domains_filename', 'enron_domains_')
#     config.set('Community Extractor', 'enron_sub_domains_count_filename', 'enron_domains_freq')
#     
#     # Writing our configuration file to 'example.cfg'
#     with open('commext.cfg', 'wb') as configfile:
#         config.write(configfile)
#     return

    malletdir = config.get('Community Extractor', 'malletdir')
    From = config.get('Community Extractor', 'fromdate')
    To = config.get('Community Extractor', 'todate')

    #Old method, replaced with mongodb search
    '''
    #     startdate = parse("2001-03-19 0:0:00-08:00")
    #     enddate = parse("2001-03-22 0:0:00-08:00")
    #     type = "BOTH"
    #     interval = "15S"
        
    #     userlist = []
    #     try:
    #         userlist = get_distinct_users()
    #     except Exception as e:
    #         print e
    #     if len(userlist) == 0:
    #         return
        #Uncomment to regenerate this file
    #     tuplelist = getTimeSortedMessageList()
    #     f = open('tuplelist','w')
    #     for tuple in tuplelist:
    #         for item in tuple:
    #             f.write(str(item)+";")
    #         f.write("\n")
    #     f.close()
        
    #     f = open("tuplelist","r")
    #     tuplelist = f.readlines()
    #     f.close()
        
        #find_and_print_intervals(startdate,enddate,tuplelist);
        
    #     find_domains()
    #     list_domains()
    #     group_into_domains()
    '''

    #domain_communication_frequency()
    From = "1 Jan 2001 0:00:00 -0700 (PDT)"
    To = "1 Jan 2002 23:59:59 -0700 (PDT)"
    #domain_communication_frequency_by_time(From=From,To=To)
    From = "1 Jan 2001 0:00:00 -0700 (PDT)"
    TZOFFSETS = {"PDT": -25200}#-7:00 = 60*60*7*-1
    From = dateutil.parser.parse(From, tzinfos= TZOFFSETS)
    for weeknum in range(1,27):
        From = From + datetime.timedelta(14,0) # days, seconds, then other fields.
        domain_communication_frequency_by_time(From=str(From), To=None, biweek=True, filename=weeknum)
    for weeknum in range(1,27):
        #Statistic for average email sent to domain biweekly
        domain_statistics("output/amount_under_domain_week_"+str(weeknum))
    print str(statdict)
    overall_total_average=0
    count = 0
    for key, item in statdict.iteritems():
        count+=1
        overall_total_average+=item['average']
         
    finalaverage = overall_total_average / float(count)
     
    #domain_communication_frequency()
    print "The overall average is",finalaverage
     
    itemcount = 0
    companies = []
    f = open('usefulcompanies','w')
    for key in sorted(statdict, key=lambda element: statdict[element]['average'], reverse=True):
         
        if statdict[key]['average']>finalaverage:
            itemcount+=1
            f.write(str(key)+":"+str(statdict[key]['average'])+";\n")
    f.close()
    print "There are these many domains with more than the average", itemcount
    #Train topics on each company and see if they are related
    fileloc = "usefulcompanies"
        # Parse the string using dateutil
    From = "1 Jan 2001 0:00:00 -0700 (PDT)"
    From = dateutil.parser.parse(From, tzinfos= TZOFFSETS)
    To = "1 Jan 2002 23:59:59 -0700 (PDT)"
    To = dateutil.parser.parse(To, tzinfos= TZOFFSETS)

    from_in_pdt = From
    to_in_pdt = To
    group_companies_by_topics(from_in_pdt, to_in_pdt, malletdir, fileloc)
if __name__ == "__main__":
    main()