from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import ast

# This MrJob computes the pagerank of a graph
#
class MRteleport(MRJob):
    def mapper_init_2nd(self):

        debug = self.options.pathName + "/debug.txt"
        #filew = self.options.pathName + "page_rank_"+str(self.options.df)+".txt"
        filew = self.options.pathName + "/part-00000"


        open(filew,"w").close()

    #Second MRjob is created to take care of teleportation and redistributing dangling mass
    def mapper_2nd(self, _, line):
        key, value = line.strip().split("\t")
        key = key.replace('"', '')
        debug = self.options.pathName + "/debug.txt"
        #--#filew = self.options.pathName + "page_rank_"+str(self.options.df)+".txt"
        filew = self.options.pathName + "/part-00000"
        alpha = 1-float(self.options.df) #Teleportation factor
        #num_nodes = 11
        ## number of nodes from counter
        num_nodes = int(self.options.G)

        with open(debug,"a+") as fd:
            fd.write(str(key+"\t"+str(value)+"\n"))

        if(key=="*"):
            value = value.replace('"', '')
            self.dangling_mass = float(value)
        else:
            node = []
            v = ast.literal_eval(value)
            new_pr_tp  =  alpha/num_nodes   #adding teleportation factor to page rank score
            try:
                new_pr_dg  = (1-alpha)*((self.dangling_mass/num_nodes) + float(v[1]))
            except AttributeError:
                # no dangling mass
                new_pr_dg = 0

            new_pr_score = new_pr_tp + new_pr_dg
            node.append(key)
            node.append(str(v[0]))
            node.append(str(new_pr_score))

            with open(filew,"a+") as fw:
                fw.writelines("\t".join(node) + "\n")

            with open(debug, "a+") as fd:
                fd.writelines("\t".join(node) + "\n")

            yield key,(v[0],new_pr_score)


    def configure_options(self):
        super(MRteleport, self).configure_options()
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="", type='str',
            help='pathName: pathname where intermedateResults.txt is stored')

        self.add_passthrough_option(
            '--df', dest='df', default="", type='str',
            help='damping factor')

        self.add_passthrough_option(
            '--G', dest='G', default="", type='str',
            help='Total Number of nodes in the whole graph')

    def steps(self):
        return [MRStep(mapper=self.mapper_2nd, mapper_init=self.mapper_init_2nd)]

if __name__ == '__main__':
    MRteleport.run()