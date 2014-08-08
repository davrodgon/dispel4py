# Copyright (c) The University of Edinburgh 2014
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
The IPython module for visualising a Dispel4Py graph using Graphviz dot.

For example, to visualise a graph named 'pipeline'::

    from dispel4py.visualisation import display
    display(pipeline)
    
'''
from verce.workflow_graph import drawDot
from IPython.core.display import display_png

def display(graph):
    '''
    Visualises the input graph.
    '''
    display_png(drawDot(graph), raw=True)    

from IPython.core.magic import (Magics, magics_class, line_magic)
