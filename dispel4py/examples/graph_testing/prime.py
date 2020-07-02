# coding: utf-8

# # Testing your installation

# If you have installed correctly dispel4py, you can easily run this first test called pipeline_test.py  This is a dispel4py graph which produces a pipeline workflow with one producer node (prod) and 5 consumer nodes.

# Remenber that first you need to have installed dispel4py:
#
#     >> pip install dispel4py
#
# For running this example:

# In[4]:

#get_ipython().system(u'dispel4py simple dispel4py.examples.graph_testing.pipeline_test -i 10')


# Now, lets test your installation with the example that you have followed during the presentation (slides - Tutorial dispel4py basic I):

# #### Check prime number

# In[8]:

from dispel4py.base import ProducerPE, IterativePE, ConsumerPE
from dispel4py.workflow_graph import WorkflowGraph
import random


class NumberProducer(ProducerPE):
    def __init__(self):
        ProducerPE.__init__(self)

    def _process(self, inputs):
        # this PE produces one input
        result = random.randint(1, 1000)
        return result


class IsPrime(IterativePE):
    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, num):
        # this PE consumes one input and produces one output
        print("before checking data - %s - is prime or not“" % num)
        if all(num % i != 0 for i in range(2, num)):
            return num


class PrintPrime(ConsumerPE):
    def __init__(self):
        ConsumerPE.__init__(self)

    def _process(self, num):
        # this PE consumes one input
        print("the num %s is prime" % num)


# In[9]:

producer = NumberProducer()
isprime = IsPrime()
printprime = PrintPrime()

graph = WorkflowGraph()
graph.connect(producer, 'output', isprime, 'input')
graph.connect(isprime, 'output', printprime, 'input')


# Running the workflow in the notebook by using a handy function called "simple_process":
# This function allows us to run a graph, by indicanting the first PE, and the number of iterations that we want to run it. e.g.
#  * simple_process(graph, {producer: 1})
#
# It runs 1 iteration of the graph
#

# In[10]:

#from dispel4py.new.simple_process import process as simple_process
#simple_process(graph, {producer: 5})


# In[ ]:
# If we want to run this example in command line, you should comment the before line "simple_process ...." and type the following in the command line:
# dispel4py simple Testing_dispel4py.py -i 5
