import pymongo
import pytz
import sys
import re
import datetime
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz
import community

from pymongo import MongoClient
from pytz import timezone
from dateutil.parser import parse
from itertools import count
from networkx.algorithms.centrality.betweenness import edge_betweenness        

def is_community(component):
    if len(component.nodes())<6:
        return True
    else:
        max = 0
        for edge,value in nx.edge_betweenness(component,normalized=False).iteritems():
            if value > max:
                max = value
        if max <= len(component.nodes())-1:
            return True
        else:
            return False

def remove_from_graph(component, G):
    for node in component.nodes():
        G.remove_node(node)
    return component

def custom_remove_edge(component):
    #remove edge
    removed_edge = None
    #check if broken
    max = 0
    for current_edge, edge_betweenness_value in nx.edge_betweenness(component).iteritems():
        if edge_betweenness_value > max:
            removed_edge = current_edge
            max  = edge_betweenness_value
    #Find edge with this value, remove it
    component.remove_edge(removed_edge[0],removed_edge[1])
    if removed_edge is None:
        raise ValueError('Removed edge should not be None')
    count = 0
    for component in list(nx.connected_component_subgraphs(component)):
        count += 1
    if count >= 2:
        return True,removed_edge
    else:
        return False,removed_edge
    


def find_communities(G):
    ###APPROACH 1###
    ###APPROACH 2###
#     G = nx.random_graphs.powerlaw_cluster_graph(300, 1, .4)
    part = community.best_partition(G)
    values = [part.get(node) for node in G.nodes()]
    nx.draw_spring(G, cmap = plt.get_cmap('jet'), node_color = values, node_size=30, with_labels=False)
    plt.show()
    #We will loop until a stop condition is reached
    #We first find all the components inside the graph
    result = []
    iter_num = 1
    for i in range(iter_num):
        componentlist = list(nx.connected_component_subgraphs(G))
        while len(G.nodes())>0:
            
            removed_edges_list = []
            for component in componentlist:
                print "Starting component"
                if is_community(component):
                    print "Found a community, removing component"
                    result.append(remove_from_graph(component,G))
                else:
                    while True:
                        print "Trying to split"
                        split, removed_edge = custom_remove_edge(component)
                        #Since this is a local removal, we have to remove it from the graph
                        removed_edges_list.append(removed_edge)
                        if split:
                            print"Success splitting"
                            break
            for edge in removed_edges_list:
                G.remove_edge(edge[0],edge[1])
            componentlist = list(nx.connected_component_subgraphs(G))    
    
    
    return result   

def main():
    client = MongoClient("localhost", 27017)
    db = client.enron_mail
    messages = db.messages
    
    
    initial_emailset =  messages.find({"$or":[{"subFolder":"sent"},{"subFolder":"sent_items"}]}).distinct("headers.From")
    
    G=nx.Graph()
    #Setup the graph with the initial nodes
    #These will be the people from enron who have messages in sent and sent_items folders
    for value in initial_emailset:
        G.add_node(value.strip())
    
    #Regex to check if enron email
    p = re.compile('enron\.com')
    #List of all messages sent
    sentmessages = messages.find({"$or":[{"subFolder":"sent"},{"subFolder":"sent_items"}]})
    
    #Statistics data
    nodenum = 1
    nodelist1 =  G.nodes()
    
    for node in nodelist1:
        print nodenum, len(nodelist1), len(G.nodes()), len(G.edges())
        nodenum = nodenum+1
#         if nodenum == 101:
#             break
        #Find all messages sent by node
        to_messages = db.messages.find({
        "$and":[
            {"headers.From":node},
            {"$or":[{"subFolder":"sent"},{"subFolder":"sent_items"}]}
        ]}
#         ,
#         {
#             "headers.To":1,
#             "headers.Cc":1
#         }
        )
        for message in to_messages:
            #See to whom they were sent
            to_list = []
            cc_list = []
            emaillist = []
            try:
                to_list = message["headers"]["To"].split(",")
                map(lambda s: s.strip(), to_list)
            except Exception as e:
                pass #no to field
            try:
                cc_list = message["headers"]["Cc"].split(",")
                map(lambda s: s.strip(), cc_list)
            except Exception as e:
                pass #no
            #Combine the To and Cc fields to get a real TO
            emaillist = to_list+cc_list
            #Now emaillist contains all the email addresses that node sent to
            for email in emaillist:
                #limit scope to within enron for size purposes.
                email.strip()
#                 if p.search(email):
                #check if node exists
                exists = False
                nasdads = G.nodes()
                #If not create it so we can connect it
                if not(email in G.nodes()):
                    G.add_node(email)
                    
                #Check if edge exists
                tofromedge = G.get_edge_data(node, email)
                if tofromedge is None:
                    #if not create one
                        dictionary = {
                            "messageammount":1,
                            "from":node,
                            "to":email,
                            "fromammount":1,
                            "toammount":0
                        }
                        G.add_edge(node, email, ds=dictionary)
                else:
                    #if yes just append email
                    e = G.edge[node][email]
                    G.edge[node][email]["ds"]["messageammount"]+=1
                    if G.edge[node][email]["ds"]["from"]==node:
                        G.edge[node][email]["ds"]["fromammount"]+=1
                    else:
                        G.edge[node][email]["ds"]["toammount"]+=1
                    
#                     if G.edge[node][email]["ammount"] > 10:
#                         print "Heavy communication!", G.edge[node][email]
#                 else:
#                     pass#print "Email passed",email
#     nx.draw(G)
#     A = nx.to_agraph(G)
#     A.layout(prog='fdp')
#     A.draw('color.png')
    pass
    
    avg_messages = 0;
    count = 0
    total = 0
    morethanten = 0
    morethan5t = 0
    morethan5f = 0
    for edge in G.edges(data=True):
        count+=1
        total+=edge[2]["ds"]["messageammount"]
        if edge[2]["ds"]["messageammount"] > 10:
            morethanten+=1
        if edge[2]["ds"]["fromammount"]>=4:
            morethan5f+=1
        if edge[2]["ds"]["toammount"]>=4:
            morethan5t+=1
#     f = open('edges','w')
#     f.writelines(G.edges(data=True))
#     f.close()
    avg_messages = total/count
    print "Average messages per person:",avg_messages
    removelist = []
    for edge in G.edges(data=True):
        if edge[2]["ds"]["messageammount"] <= 10:
            removelist.append(edge)
    for edgetoremove in removelist:
        G.remove_edge(edgetoremove[0], edgetoremove[1])
    print "Trimmed edges",len(G.edges())
    
    for node in G.nodes():
        if G.degree(node) == 0:
            G.remove_node(node)
    
    print "Nodes:",len(G.nodes()),"Edges:",len(G.edges())
    
    nx.draw(G)
    plt.show()
    
    communities = find_communities(G)
    splitgraph = nx.Graph()
    for community in communities:
        print "Printing a community"
        splitgraph.add_nodes_from(community)
        splitgraph.add_nodes_from(community.edges())
    nx.draw(splitgraph)
    
    
    f = open("Communities","w")
    for community in communities:
        f.write(str(communities))
    f.close()
        
    
    

if __name__ == "__main__":
    main()