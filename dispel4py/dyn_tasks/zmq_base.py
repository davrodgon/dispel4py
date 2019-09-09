'''
Requirements:

    pip install pyzmq

Example:

    python -m dispel4py.new.processor zmq_multi dispel4py/examples/graph_testing/pipeline_test.py -i 10 -n 5

where
-n <INT> is the number of parallel processes
-i <INT> is the number of iterations

Other parameters:
-d <JSON> input data
-f <PATH> input data file

'''

import argparse
import copy
import
import multiprocessing
import uuid
from dispel4py.new import processor
import time
import zmq
from zmq.devices.basedevice import ProcessDevice



class ZMQProducer():
    def __init__(self, hostname, port,value_serializer=msgpack.packb):
        try:
            host_remote = socket.gethostbyaddr(hostname)[0]
        except:
            # print('Hostname ', hostname, ' not valid, reverting to localhost')
            host_remote = 'localhost'
        try:
            host_local = socket.gethostbyaddr(socket.gethostname())[0]
        except:
            # print('gethostname ', socket.gethostname(), ' not valid, reverting to localhost')
            host_local = 'localhost'

        self.host_remote = host_remote
        self.port = port
        self.value_serializer=value_serializer

        if (host_remote == host_local):
            url_connect = "ipc:///tmp/dispel4py/%d" % self.port
        else:
            url_connect = "tcp://%s:%d" % (self.host_remote, self.port)
        context = zmq.Context()

        self.socket = context.socket(zmq.PUSH)
        self.socket.connect("tcp://127.0.0.1:%d" % port)

    def write(self,value):
        msg = msgpack.packb(value, use_bin_type=True)    
        self.socket.send(msg)

    def send(self,topic,value):
        self.write(value=value)



class ZMQConsumer():
    def __init__(self,port,value_serializer=msgpack.packb):
        self.port = port
        self.value_serializer=value_serializer
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.connect("tcp://127.0.0.1:%d" % port)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            message = self.socket.recv()
            # print ("Consumer got message: %s" % message)
            value = msgpack.unpackb(message, encoding='utf-8')
            # print ("Unpacked: %s" %  value)
        except IndexError:
            raise StopIteration
        return value

 
class GenericWriter():
    def __init__(self, producer, pe_id, output_name):
        self.producer = producer
        self.pe_id = pe_id
        self.output_name = output_name

    def write(self, data):
        destinations = map_output(workflow.graph, self.pe_id, self.output_name)
        if not destinations:
            print('Output collected from {}: {}'.format(self.pe_id, data))
        for dest_id, input_name in destinations:
            self.producer.send(topic, value=(dest_id, {input_name: data}))


def _processWorker(topic, proc, workflow):
    frontend_port = 5559
    backend_port = 5560
    pes = {node.getContainedObject().id: node.getContainedObject() for node in workflow.graph.nodes()}
    nodes = {node.getContainedObject().id: node for node in workflow.graph.nodes()}
    producer = ZMQProducer(port=frontend_port)
    consumer = ZMQConsumer(port=backend_port)
    while True:
        try:
            value = next(consumer)
            # print('Message from consumer ', value)
        except StopIteration:
            return None

        try:
            pe_id, data = value
            print('{} receiver input: {}'.format(pe_id,data))
            pe = pes[pe_id]
            for o in pe.outputconnections:
                pe.outputconnections[o]['writer'] = GenericWriter(producer, pe_id, o)
            output = pe.process(data)
            print('{} writing output: {}'.format(pe.id, output))
            for output_name, output_value in output.items():
                destinations = map_output(workflow.graph, nodes[pe_id], output_name)
                if not destinations:
                    print('Output collected from {}: {}'.format(pe_id, output_value))
                for dest_id, input_name in destinations:
                    producer.send(topic, value=(dest_id, {input_name: output_value}))
        except Exception as e:
            print(e)
            pass


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        prog='dispel4py',
        description='Submit a dispel4py graph to multiprocessing.')
    parser.add_argument('-ct', '--consumer-timeout',
                        help='stop consumers after timeout in ms',
                        type=int)
    parser.add_argument('-n', '--num', metavar='num_processes', required=True,
                        type=int, help='number of processes to run')
    parser.add_argument('-t', '--topic', default=str(uuid.uuid4()),
                        help='Kafka topic name')
    result = parser.parse_args(args, namespace)
    return result



