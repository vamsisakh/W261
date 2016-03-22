from numpy import random,array
from pg_rank import page_rank
from MRteleport import MRteleport
import sys
import ast
import os
import json

damping_factor = [0, 0.25, 0.5,0.75, 0.85, 1]  

LOCAL_HOME = "/Users/Safyre/Dropbox/MIDS/W261/Week9"

for df in damping_factor:
    fread = 'PageRankTest.txt'
    #fwrite = 'page_rank_'+str(df)+'.txt'
    fwrite = 'part-00000'
    NEW_OUT_DIR = LOCAL_HOME + "/output_"+ str(df)
    
    # Allow ourselves to skip calculating G after the first time
    first_run = True

    # remove the file if it exists from previous iterations
    if os.path.exists(fwrite) and os.path.isfile(fwrite):
        os.remove(fwrite)

    if os.path.isdir(NEW_OUT_DIR):
        for mr_file in os.listdir(NEW_OUT_DIR):
            os.remove(NEW_OUT_DIR+ "/"+mr_file)
    else:
        os.mkdir(NEW_OUT_DIR)
    
    # relocate the file address into the new folder
    fwrite = NEW_OUT_DIR + "/" +fwrite
    print "Writing initialized PR file to: "+ fwrite
    
    #Initializing the page ranks based on the adjacency list
    #Initial page rank for each page is just 1/N , where N is the number of nodes in the graph
    node_count = 0
    with open(fread, 'r') as fr:
        nodes = set()
        for fl in fr:
            f = fl.split("\t")
            nodes.add(f[0])
            edges = ast.literal_eval(f[1])
            for e in edges.keys():
                nodes.add(e)
            
    node_count = len(nodes)

    with open(fread,'r') as fr:
        for f in fr:
            ln = f.strip().split("\t")
            ln.append((str((float(1)/node_count))))
            nodes.remove(ln[0])
            with open(fwrite,'a+') as fw:
                fw.writelines("\t".join(ln) + "\n")

    while(len(nodes)!=0):
        dangling = nodes.pop()
        n = [dangling,"NULL",str(float(1)/node_count)]
        with open(fwrite,'a+') as fw:
            fw.writelines("\t".join(n) + "\n")  
         
    mr_job1 = page_rank(args=[fwrite,
                              '--pathName',
                              NEW_OUT_DIR,
                              '--output-dir',
                              NEW_OUT_DIR,
                              '--df',df]) 

    i = 1

    threshold = 0.005
    while(i<20):
        print "\n","iteration ",i,"\n"
        print "\n","Damping Factor ",df,"\n"

        unvisited_nodes = 0
        with mr_job1.make_runner() as runner: 
            runner.run()

        if first_run:
            list_counters = runner.counters()
            for counter in list_counters:
                G = counter['group']['G']
                print G
            first_run = False

        mr_job2 = MRteleport(args=[fwrite,
                              '--pathName',
                              NEW_OUT_DIR,
                              '--df',df,
                              '--output-dir',
                                   NEW_OUT_DIR,
                                '--G', G])
        with mr_job2.make_runner() as runner2:
            runner2.run()
            
            #stream_output: get access of the output
            #for line in runner2.stream_output():
            #    key, value =  mr_job2.parse_output_line(line)
            #    print key, value

        i +=1

print "Done- Page-Ranks for all nodes have been computed"