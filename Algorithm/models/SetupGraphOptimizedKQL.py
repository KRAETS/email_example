'''
Created on Mar 17, 2015

@author: pe25171
'''
import datetime, time
from itertools import count
import json
import re
from subprocess import call
import sys
import timeit

import community
from dateutil.parser import parse
from igraph import * 
from networkx.algorithms.centrality.betweenness import edge_betweenness        
import pygraphviz
from pymongo import MongoClient
import pymongo
from pytz import timezone
import pytz 
import math
import matplotlib.pyplot as plt
from test.test_pprint import list2 


def setupGraph(nodestopnum=None):
    c1 =0
    c2=0 
    c3=0
    f = open('statistics','a')
    
    #initial_emailset =  messages.find({"$or":[{"message_folder":"sent"},{"message_folder":"sent_items"}]}).distinct("sender_address")
    start1 = timeit.default_timer()


    f.write('First query start time:'+str(start1))
    f.write('\n')

    #REPLACEMENT
    
    call(["java", "-jar", "kql_engine-1.0-SNAPSHOT-jar-with-dependencies.jar","-kqlq","SELECT DISTINCT \ALL*email_address*_:source\ FROM \ALL/emailmessage}\ WHERE ( \ALL*folder\ = 'sent_items' OR \ALL*folder\ = 'sent')",'-fol','5'])
    c1= c1+1
    #print "Done"
    with open("output/outfile") as data_file:    
        data = json.load(data_file)
    #print data
    start2 = timeit.default_timer()

    f.write('First query finish time:'+str(start2))
    f.write('\n')
    f.write('First query total time:'+str(start2-start1))
    f.write('\n')
    G=Graph() 
#    Setup the graph with the initial nodes
#    These will be the people from enron who have messages in sent and sent_items folders
    G.add_vertices(len(data['results']))
    print G
    counter = 0
    for value in data['results']:
        G.vs[counter]["email"]=value['sender_address']
        counter+=1
     
    #Regex to check if enron email
    p = re.compile('enron\.com')
    #List of all messages sent
#     query = "select \\ALL*_:email_message\\ FROM \\ALL/{folder}/_:email_message\\ WHERE \\ALL*folder*_:email_message\\ = 'sent_items' OR \\ALL*folder*_:email_message\\ = 'sent'"
#     call(["java", "-jar", "kql_engine-1.0-SNAPSHOT-jar-with-dependencies.jar","-kqlq",query])
# 
#     with open("output/outfile") as data_file:    
#         data = json.load(data_file)
#     sentmessages = data['results']
     
    #Statistics data
    nodenum = 1
    nodelist1 =  G.vs["email"]
    querylist = []
    nodelist2 = []
    #Filter malformed emails
    for node in nodelist1:
        if "'" in node or "<" in node or " " in node:
            continue
        nodelist2.append(node)
    #Setup QUERIES
    nodestopnum = math.floor((6.0/6)*len(nodelist2))
    for node in nodelist2:
#         print nodenum, len(nodelist1), len(G.vs), len(G.es)
        nodenum = nodenum+1
        if nodestopnum is not None:
            if nodenum == nodestopnum:
                break
        #Find all messages sent by node
        start1 = timeit.default_timer()

        query = '''select \\ALL*[emailmessage]\\ FROM \\ALL/emailmessage\\ WHERE ( \\ALL*folder*_:email_message\\ = 'sent_items' OR \\ALL*folder*_:email_message\\= 'sent') AND ( \\ALL*email_address*_:sender\\=\''''+node+"\' )"
        c2=c2+1
        querylist.append(query)
        print len(nodelist2)

    
    #Time them
    f.write('Mega query start time:'+str(start1))
    f.write('\n')
    arglist = ["java", "-jar", "kql_engine-1.0-SNAPSHOT-jar-with-dependencies.jar",'-fol','0',"-ql"]
    finalarglist =arglist+querylist
    call(finalarglist)
    
    start2 = timeit.default_timer()

    f.write('Sub query finish time:'+str(start2))
    f.write('\n')
    f.write('Sub query total time:'+str(start2-start1))
    f.write('\n')
    filenumber = 0
    print c1,c2,c3
    return
    #Build graph
    nodenum = 0;
    for node in nodelist2:
        print len(nodelist2)
        print nodenum, len(nodelist2), len(G.vs), len(G.es)
        f.write(str(nodenum) +" "+ str(len(nodelist2)) + " "+ str( len(G.vs))+ " "+ str(len(G.es)))
#         nodestopnum = 6
        nodenum = nodenum+1
        if nodestopnum is not None:
            if nodenum == nodestopnum:
                break
        #Find all messages sent by node
        start1 = timeit.default_timer()


        
        
        with open("output/outfile"+str(filenumber)) as data_file:    
            data = json.load(data_file)
        to_messages = data['results']
            
#         to_messages = db.messages.find({
#         "$and":[
#             {"headers.From":node},
#             {"$or":[{"subFolder":"sent"},{"subFolder":"sent_items"}]}
#         ]}
#         ,
#         {
#             "headers.To":1,
#             "headers.Cc":1
#         }
#         )
        for message in to_messages:
            #See to whom they were sent
#             print "Starting message"
            to_list = []
            #cc_list = []
            emaillist = []
            try:
                to_list = message["recipient_address"].split(",")
                map(lambda s: s.strip(), to_list)
            except Exception as e:
                pass #no to field
#             try:
#                 cc_list = message["recipient_address"].split(",")
#                 map(lambda s: s.strip(), cc_list)
#             except Exception as e:
#                 pass #no
            #Combine the To and Cc fields to get a real TO
            emaillist = to_list#+cc_list
            #Now emaillist contains all the email addresses that node sent to
            for email in emaillist:
                #limit scope to within enron for size purposes.
                email.strip()
#                 print "Starting email"
#                 if p.search(email):
                #check if node exists
                exists = False
                for n in G.vs:
                    #print n["email"]
                    if n["email"] == email:
                        exists = True
                        break
                #If not create it so we can connect it
                if not(exists):
                    G.add_vertex(email,email=email.strip())
                     
                v1 = None
                for n in G.vs:
                    if n["email"] == node:
                        v1 = n
                        break
                      
                v2 = None
                for n in G.vs:
                    if n["email"] == email:
                        v2 = n 
                        break
                #Check if edge exists
#                 print "Working edge"
                tofromedge = G.get_eid(v1, v2, directed=False, error=False)
                if tofromedge is -1:
                    #if not create one
                        dictionary = {
                            "messageammount":1,
                            "from":node.strip(),
                            "to":email.strip(),
                            "fromammount":1,
                            "toammount":0
                        }
                        G.add_edge(v1, v2, ds=dictionary)
                else:
                    #if yes just append email
                    G.es[tofromedge]["ds"]["messageammount"]+=1
                    if G.es[tofromedge]["ds"]["from"]==node:
                        G.es[tofromedge]["ds"]["fromammount"]+=1
                    else:
                        G.es[tofromedge]["ds"]["toammount"]+=1
                     
        filenumber+=1
    f.close()
    print c1,c2,c3

    return G

def main():
    f = open('statistics','a')
    start1 = timeit.default_timer()
    f.write("Original Start time:"+str(start1));
    f.write('\n')
    f.close()
    G = setupGraph()
    f = open('statistics','a')
    
    start2 = timeit.default_timer()
    f.write("Finished setup time:"+str(start2));
    f.write('\n')
    
    diff = start2 - start1
    print "The diff is",diff
    f.write('The total time is:'+str(diff))
    
    avg_messages = 0;
    count = 0
    total = 0
    morethanten = 0
    morethan5t = 0
    morethan5f = 0
    
    for edge in G.es:
        count+=1
        #print edge
        total+=edge["ds"]["messageammount"]
        if edge["ds"]["messageammount"] > 10:
            morethanten+=1
        if edge["ds"]["fromammount"]>=4:
            morethan5f+=1
        if edge["ds"]["toammount"]>=4:
            morethan5t+=1

    #Find the average ammount of messages per person
    if count == 0:
        count = 1
        
    avg_messages = total/count
    print "Average messages per person:",avg_messages
    f.write("Average"+str(avg_messages))
    removelist = []
    for edge in G.es:
        if edge["ds"]["messageammount"] <= 16:
            removelist.append(edge)
    G.delete_edges(removelist)
    
#     #Eliminate unidirectionality
#     removelist = []
#     for edge in G.es:
#         if edge["ds"]["fromammount"] <= 3:
#             removelist.append(edge)
#     G.delete_edges(removelist)
#     
#     removelist = []
#     for edge in G.es:
#         if edge["ds"]["toammount"] <= 3:
#             removelist.append(edge)
#     G.delete_edges(removelist)
#     
    #Remove lonely vertices
    removelist = []
    for node in G.vs:
        if G.degree(node.index) == 0:
            removelist.append(node)
    G.delete_vertices(removelist)
      
    print "Nodes:",len(G.vs),"Edges:",len(G.es)
#Sample graph
#     G.add_vertices(11)
#     G.add_edge(0, 1)
#     G.add_edge(1, 2)
#     G.add_edge(2, 0)
#     G.add_edge(2, 4)
#     
#     G.add_edge(4, 5)
#     G.add_edge(4, 6)
#     G.add_edge(4, 7)
#     G.add_edge(4, 3)
#     G.add_edge(3, 6)
#     G.add_edge(3, 7)
#     G.add_edge(3, 5)
#     G.add_edge(5, 6)
#     G.add_edge(5, 7)
#     
#     G.add_edge(8, 9)
#     G.add_edge(9, 10)
#     G.add_edge(10, 8)
#     G.add_edge(6, 8)

    
    layout = G.layout("auto")
    plot(G, layout = layout)
    
    #Method #1
    result = G.community_edge_betweenness(directed=False, weights=None)

    print "Writing communities..."
    f = open('communities','w')
    f.write("Nodes:\n")
    for node in G.vs:
        f.write(node["email"].strip()+"\n")
    f.write("Edges")
    for edge in G.es:
        f.write("Edge:"+str(edge["ds"])+"\n")
    f.close()
    print "Done"
    print "Transforming to clusters..."
    result2 = result.as_clustering()
    print "Done... Writing cluster membership..."
    f = open('membership','w')
    for cluster in result2.subgraphs():
        f.write("Cluster:\n")
        for node in cluster.vs:
            f.write( node["email"].strip()+"\n")
    f.close()
    print "Done"
    print "Outputting final cluster graph..."
    final = result2.cluster_graph()
    plot(final, layout=layout)
    print "Done"
    start3 = timeit.default_timer()
    
    diff2 = start3 - start1
    
    f = open('statistics','a')
    f.write("The completion time was:"+str(start3))
    f.write('\n')
    f.write('The total runtime was:'+str(diff2))
    f.write('\n')
    f.close()
if __name__ == '__main__':
    main()