from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import ast

# This MrJob computes the pagerank of a graph
#
class page_rank(MRJob):
    def mapper(self, _,line):
        ln = line.split("\t")
        key = ln[0].replace('"', '')
        """input after 1st iteration looks like:
        "A"	["NULL", 0.09090909090909091]
        "B"	["{'C': 1}", 0.09090909090909091]

        part-00000:
        ========================================
        B	{'C': 1}	0.0909090909091
        C	{'B': 1}	0.0909090909091
        D	{'A': 1, 'B': 1}	0.0909090909091
        E	{'D': 1, 'B': 1, 'F': 1}	0.0909090909091
        F	{'B': 1, 'E': 1}	0.0909090909091
        G	{'B': 1, 'E': 1}	0.0909090909091
        H	{'B': 1, 'E': 1}	0.0909090909091
        I	{'B': 1, 'E': 1}	0.0909090909091
        J	{'E': 1}	0.0909090909091
        K	{'E': 1}	0.0909090909091
        A	NULL	0.0909090909091

        After the second iteration:
        ln = ['"A"', '["NULL", 0.0909....]']

        part-00000
        ===============
        "A"	["NULL", 0.09090909090909091]
        "B"	["{'C': 1}", 0.09090909090909091]
        "C"	["{'B': 1}", 0.09090909090909091]
        "D"	["{'A': 1, 'B': 1}", 0.09090909090909091]
        "E"	["{'B': 1, 'D': 1, 'F': 1}", 0.09090909090909091]
        "F"	["{'B': 1, 'E': 1}", 0.09090909090909091]
        "G"	["{'B': 1, 'E': 1}", 0.09090909090909091]
        "H"	["{'B': 1, 'E': 1}", 0.09090909090909091]
        "I"	["{'B': 1, 'E': 1}", 0.09090909090909091]
        "J"	["{'E': 1}", 0.09090909090909091]
        "K"	["{'E': 1}", 0.09090909090909091]
        """
        values = ln[1].replace('\\', '')

        if values != "NULL":

            edges = values.replace('"', '')

            try:
                edges = ast.literal_eval(edges)
                # check if it's a dictionary
                if not hasattr(edges, 'keys'):
                    # if not, it might be a list where
                    # the first item is the neighbors dictionary
                    #need to fix ln here as well

                    ln = list()
                    ln.extend(key)
                    ln.extend([i for i in edges])
                    edges = edges[0]

            except ValueError:
                edges = values.replace("'", "")
                edges = ast.literal_eval(edges)
                # redefine ln
                # works for second+ iteration of dangling nodes
                # but not for those that have neighbors
                ln = list(key)
                ln.extend([i for i in edges])
                # if edges becomes a list--will happen
                # if neighbors is "NULL"
                if not hasattr(edges, 'keys'):
                    if edges[0] == "NULL":
                        edges = {}
                    else:
                        edges = ast.literal_eval(edges[0].replace('"',''))

        else:
            edges = {}

        num_edges = len(edges)

        #if not page_rank:
        page_rank = float(ln[2])


        debug = self.options.pathName + "/debug.txt"

        for e in edges.keys():
            with open(debug,"a+") as fd:
                fd.write(str(e +" "))
                fd.write(str(str((float(page_rank)/float(num_edges)))+" "))
                fd.write(' P')
                fd.write("\n")

        with open(debug,"a+") as fd:
            fd.write(str(ln[0])+" ")
            fd.write(str(ln[1])+" ")
            fd.write(" G")
            fd.write("\n")

        # increment counter to get number of nodes
        self.increment_counter('group','G',1)

        #This node will equally distribute its pagerank among each of its edges
        #Key = Edge , Value = page_rank_mass is emitted for each of the edges
        for e in edges.keys():
            yield e,((float(page_rank)/float(num_edges)),'P')  ##emitting the page rank mass

        yield key,((edges, 'G', ln[2])) #Emitting the graph structure

    def reducer_init(self):
        #filew = self.options.pathName + "page_rank_"+str(self.options.df)+".txt"
        filew = self.options.pathName + "/part-00000"


        debug = self.options.pathName + "/debug.txt"

        open(filew,"w").close()
        with open(debug,"a+") as fd:
            fd.write("End of Map-phase")

    #Reducer will sum up all the pageranks
    def reducer(self,key,value):

        node = []
        page_rank = 0
        graph = []
        dangling_node = False
        for v in value:
            if(v[1]=='P'):   #Page Rank Mass
                page_rank += (v[0])
            elif(v[1]=='G'): # Graph structure
                if v[0]:
                    graph = [key,v[0]]
                else:
                    dangling_node = True
                    pr_dangling = v[2]

        for g in graph:
            node.append(str(g))
        node.append(str(page_rank))

        #--#filew = self.options.pathName + "page_rank_"+str(self.options.df)+".txt"
        filew = self.options.pathName + "/part-00000"

        if(dangling_node==False):
            with open(filew,"a+") as fw:
                fw.writelines("\t".join(node) + "\n")

            yield key,node[1:]

        else: #Computing the dangling mass
            yield "*",pr_dangling
            yield key,("NULL",page_rank)


    def configure_options(self):
        super(page_rank, self).configure_options()
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where intermedateResults.txt is stored')

        self.add_passthrough_option(
            '--df', dest='df', default="", type='str',
            help='damping factor')

    def steps(self):
        return [MRStep(mapper=self.mapper,reducer_init = self.reducer_init,
                          reducer=self.reducer)]

if __name__ == '__main__':
    page_rank.run()