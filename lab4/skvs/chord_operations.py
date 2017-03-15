import sys
import requests as req
import socket
import random as r
from collections import Counter

# from skvs.models import KvsEntry
# This can't be imported directly into here because of how Django works,
# but is imported into here from apps.py, so you can use it.

# Code contained herein based upon pseudocode from the Chord paper:
# http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf


SIZE = 2**31-1

# borrowed, slightly optimized helper function from
# https://github.com/gaston770/python-chord/blob/master/address.py
def in_range(c, a, b):
    """
    Is c in [a,b)?, if a == b then it assumes a full circle on the DHT,
    so it returns True.
    """

    a %= SIZE
    b %= SIZE
    c %= SIZE
    if a < b:
        return a <= c < b
    return a <= c or c < b


# ## This is initialized at Django startup (see apps.py)
localNode = None

class Node(object):
    """
    A class representing our instance, for reference by other nodes
    """



    def __init__(self, address, is_remote=True):
        # store IP address
        self.address = address
        # initialize an empty successor
        self.__successor = None
        # initialize an empty predecessor
        self.__predecessor = None
        self.partition_id = r.randint(1,50) # FIGURE THIS OUT LATER
        self.counter = Counter()
        self.counter[self.partition_id] = 0

    def id(self):
        return hash(self.address) % SIZE

    # determine if this key is stored on our node
    def is_mine(self, key):
        key_location = hash(key) % SIZE
        return in_range(key_location, self.predecessor().id(), self.id())

    # query successor's ip so we can forward request
    def get_successor_ip(self):
        return self.successor().address

    def is_local(self):
        return self == localNode

    def is_remote(self):
        return self != localNode

    # ## Code for getting and setting successor and predecessor.
    # ## Use these for setting and getting, do NOT use the assignment operation on __successor or __predecessor

    def successor(self):
        """
        Returns the successor node to this node.
        If this node is a remote node, it queries the remote location and returns that location's successor.
        If this node is a local node, it just returns the known successor
        """
        if self.is_local():
            return self.__successor
        else:
            return get_successor(self.address)

    def predecessor(self):
        """
        Returns the predecessor node to this node.
        If this node is a remote node, it queries the remote location and returns that location's predecessor.
        If this node is a local node, it just returns the known predecessor.
        """
        if self.is_local():
            return self.__predecessor
        else:
            return get_predecessor(self.address)

    def set_successor(self, node):
        """
        Sets the successor node of this node, given and address
        If this node is a remote node it queries the remote location and tells it to set its successor
        If this node is a local node, it just sets the local successor.
        """
        if node == None:
            print >> sys.stderr, 'Setting successor of', socket.gethostbyname(socket.gethostname()), 'to None'
        elif self.is_local():
            print >> sys.stderr, 'Setting successor of ', socket.gethostbyname(socket.gethostname()), 'to', node.address
            self.__successor = node
        else:
            print >> sys.stderr, 'Setting remote successor of', self.address
            post_successor(self.address, node)

    def set_predecessor(self, node):
        """
        Sets the successor node of this node, given and address
        If this node is a remote node it queries the remote location and tells it to set its successor
        If this node is a local node, it just sets the local successor.
        """
        if node == None:
            print >> sys.stderr, 'Setting predecessor of', socket.gethostbyname(socket.gethostname()), 'to None'
        elif self.is_local():
            print >> sys.stderr, 'Setting predecessor of ', socket.gethostbyname(socket.gethostname()), 'to', node.address
            self.__predecessor = node
        else:
            post_predecessor(self.address, node)

    # ######## Code for finding successor node of an identifier ######## #

    # have this Node find the successor of a given slot
    def find_successor(self, key):
        desired_node = self.find_predecessor(key)
        return desired_node.successor()

    # have this Node find the predecessor of a given slot
    def find_predecessor(self, key):
        if self.is_local():
            desired_node = self
            while not desired_node.is_mine(key):
                desired_node = desired_node.successor()
            return desired_node.predecessor()
        else:
            res = req.get('http://' + self.address + '/kvs', params={'request': 'predecessor'}, data={'ip_port': key})
            # res comes in as {'address': ip_port}
            return Node(**res.json())

    # return closest node that precedes the id
    # NOTE: For now, does the same thing as find_predecessor. This will change when implenting chord
    def closest_preceding_node(self, key):
        return self.find_predecessor(key)

    # ######## Code for joining the distributed hash table ######## #

    def join(self, existing_node):
        mySuccessor = existing_node.find_successor(self.address)
        myPredecessor = mySuccessor.predecessor()
        self.__successor = mySuccessor
        self.__predecessor = myPredecessor
        self.predecessor().set_successor(self)
        self.successor().set_predecessor(self)
        # Our successor has some of our keys. Tell it to give them to us
        self.successor().notify(self)

    # update to notify successor of new node about its predecessor
    # NOTE: Not needed in linear hashing. Implement this once we get to chord.
    # def stabilize(self):
    #     x = self.successor().predecessor()
    #     # if a predecessor exists, lies between us and our successor, and we are not the only node in the ring
    #     if x is not None and in_range(x.id(), self.id(), self.successor().id()) and self.id() != self.successor().id():
    #         # we can say our successor is our successor's predecessor
    #         self.set_successor(x)
    #     # We notify our new successor about us
    #     self.successor().notify(self)

    # new_node may be our predecessor
    def notify(self, new_node):
        """
        Sets our predecessor to be the given node
        Checks if we have any entries that belong to our predecessor and, if so, sends them along.
        """
        if self.is_local():
            print >> sys.stderr, "Notified local"
            predecessor = self.predecessor()
            for entry in KvsEntry.objects.all():
                if not self.is_mine(entry.key):
                    self.sendKVSEntry(predecessor, entry.key, entry.value)
        else:
            notify(self.address, new_node)

    def sendKVSEntry(self, node, key, value):
        """
        Sends the KVS Entry to the given node
        """
        sendKVSEntry(node.address, key, value)


# Communication functions
def get_successor(address):
    """
    Asks the node at a given IP for its successor, returns that node.
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'successor'})
    # res comes in as {'address': ip_port}
    return Node(**res.json())


def get_predecessor(address):
    """
    Asks the node at a given IP for its predecessor, returns that node.
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'predecessor'})
    # res comes in as {'address': ip_port}
    return Node(**res.json())


def post_successor(address, node):
    """
    Tells address that its new successor is node
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'successor'},
             data={'ip_port': node.address})


def post_predecessor(address, node):
    """
    Tells address that its new predecessor is node
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'predecessor'},
             data={'ip_port': node.address})


def invite_to_join(address):
    """
    Tells address to join us
    """
    return req.post('http://' + address + '/kvs',
                    params={'request': 'joinme'},
                    data={'ip_port': localNode.address})


def sendKVSEntry(address, key, value):
    """
    Sends PUT request to given address asking to add the key and value
    """
    req.put('http://' + address + '/kvs/' + key, data={'val': value})


def notify(address, node):
    """
    Notifies address of its new predecessor, node
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'notify'},
             data={'ip_port': node.address})
