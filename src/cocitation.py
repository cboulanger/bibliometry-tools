from src.graphdb import GraphDb, Work, Creator


class Cocitation:
    graphdb: GraphDb = None
    cite_dict = {}
    edge_dict = {}
    work: Work = None
    creator: Creator = None

    def __init__(self, graphdb, work: Work, creator: Creator):
        if not (graphdb and work and creator):
            raise ValueError("Insufficient arguments")
        self.graphdb = graphdb
        self.work = work
        self.creator = creator

    def calculate(self):
        query = [f"match (w1:{self.work.LABEL})-[r:{self.work.REL_CITES}]->(w2:{self.work.LABEL})",
                 f"with w1, w2",
                 f"match (a1:{self.creator.LABEL})-[r1:{self.creator.REL_CREATOR_OF}]->(w1),",
                 f"      (a2:{self.creator.LABEL})-[r2:{self.creator.REL_CREATOR_OF}]->(w2)",
                 f"with w1, w2, a1, a2",
                 f"return a1.{self.creator.PROP_DISPLAY_NAME}, w1.{self.work.PROP_PUBLICATION_YEAR},",
                 f"       a2.{self.creator.PROP_DISPLAY_NAME}, w2.{self.work.PROP_PUBLICATION_YEAR}",
                 f"limit 100"]
        print("\n".join(query))
        #citations = self.graphdb.run_query(query)

# article_list=open('topfourcites.txt').read()
# article_list=article_list.replace('\n   ','\t')
# article_list=article_list.split('\n')
# cite_list=[ab[3:] for ab in article_list if ab[:2]=="CR"]
# cite_list=[[c for  c in cite.split('\t')] for cite in cite_list]


#
# for cites in cite_list:
#     cite_list=[]
#     for cite in cites:
#         split=cite.split(', ')
#         try:
#             id=split[0].upper().replace(' ',' ').replace('.','')+' '+split[1]
#             ids=id.split()
#             for i in ids:
#                 if len(i)>2:
#                     new=i[0].upper()+i[1:].lower()
#                     id=id.replace(i,new)
#         except:
#             print 'Eror with ',split
#         else:
#
#             if id not in cite_list:
#                 if id in cite_dict:
#                     cite_dict[id]=cite_dict[id]+1
#                 else:
#                     cite_dict[id]=1
#
#                 if len(cite_list)>0:
#                     for cite in cite_list:
#                         if (id,cite) in edge_dict:
#                             edge_dict[(id,cite)]=edge_dict[(id,cite)]+1
#                         elif (cite,id) in edge_dict:
#                             edge_dict[(cite,id)]=edge_dict[(cite,id)]+1
#                         else:
#                             edge_dict[(id,cite)]=1
#                 cite_list.append(id)
#                 print id
# '''
#
# import csv
# writer=csv.writer(open('cite_edge_list.csv','wb'))
#
# writer.writerow(('Source','Target','Weight','Type'))
# for edge in edge_dict:
#     if edge_dict[edge]>2 and cite_dict[edge[0]]>9 and cite_dict[edge[1]]>9 :
#         writer.writerow((edge[0],edge[1],edge_dict[edge],'Undirected'))
# '''
# import networkx as nx
# from networkx.readwrite import d3_js
# import re
# G=nx.Graph()
# counter=0
# for edge in edge_dict:
#     if edge_dict[edge]>3 and cite_dict[edge[0]]>=8 and cite_dict[edge[1]]>=8 :
#         G.add_edge(edge[0],edge[1],weight=edge_dict[edge])
#         counter=counter+1
#         print counter
#
# for node in G:
#     G.add_node(node,freq=cite_dict[node])
#
# import community
#
# partition=community.best_partition(G)
#
# print len(partition)
#
# for node in G:
#     G.add_node(node,freq=cite_dict[node], group=str(partition[node]))
#
# d3_js.export_d3_js(G,files_dir="netweb",graphname="cites",node_labels=True,group="group")
#
# fix=open('netweb/cites.json','rb').read()
# for n in G:
#     try:
#         fix=re.sub(str(n)+'''"''',str(n)+'''" , "nodeSize":'''+str(cite_dict[n]),fix)
#     except:
#         print 'error with',n
# f = open('netweb/cites.json', 'w+')
# f.write(fix)
# f.close()
