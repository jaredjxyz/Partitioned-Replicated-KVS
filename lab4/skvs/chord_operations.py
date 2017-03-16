import sys
import requests as req
import socket

# from skvs.models import KvsEntry
# This can't be imported directly into here because of how Django works,
# but is imported into here from apps.py, so you can use it.

# Code contained herein based upon pseudocode from the Chord paper:
# http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf


SIZE = 2**31 - 1

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


def double_hash(x):
    """Hashes anything twice for better distribution of hashes"""
    return hash(str(hash(str(x))))


class Node(object):
    """
    A class representing our instance, for reference by other nodes
    """

    def __init__(self, address, partition_id=None):
        # store IP address
        self.address = address
        # initialize an empty successor
        self.__successors = []
        # initialize an empty predecessor
        self.__predecessors = []
        # initialize empty partition members
        self.__partition_members = []

        self.partition_id = partition_id

    def id(self):
        return double_hash(self.partition_id) % SIZE

    # determine if this key is stored on our node
    def is_mine(self, key):
        key_location = double_hash(key) % SIZE
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

    def successors(self):
        """
        Returns the successor node to this node.
        If this node is a remote node, it queries the remote location and returns that location's successor.
        If this node is a local node, it just returns the known successor
        """
        if self.is_local():
            return self.__successors
        else:
            return get_successors(self.address)

    def predecessors(self):
        """
        Returns the predecessor node to this node.
        If this node is a remote node, it queries the remote location and returns that location's predecessor.
        If this node is a local node, it just returns the known predecessor.
        """
        if self.is_local():
            return self.__predecessors
        else:
            return get_predecessors(self.address)

    def partition_members(self):
        """
        Returns a list of all members of this partition, including self
        """
        if self.is_local():
            return self.__partition_members
        else:
            return get_partition_members(self.address)

    def set_successor(self, node):
        """
        Sets the successor node of this node, given and address
        If this node is a remote node it queries the remote location and tells it to set its successor
        If this node is a local node, it just sets the local successor.
        """
        if self.is_local():
            self.__successors.append(node)
        else:
            post_successor(self.address, node)

    def set_successors(self, nodes):
        """
        Sets multiple successors
        """
        if self.is_local():
            self.__successors.extend(nodes)
        else:
            for node in nodes:
                post_successor(self.address, node)

    def set_predecessor(self, node):
        """
        Sets the successor node of this node, given and address
        If this node is a remote node it queries the remote location and tells it to set its successor
        If this node is a local node, it just sets the local successor.
        """
        if self.is_local():
            self.__predecessors.append(node)
        else:
            post_predecessor(self.address, node)

    def set_predecessors(self, nodes):
        """
        Sets multiple predecessors of this node
        """
        if self.is_local():
            self.__predecessors.extend(nodes)
        else:
            for node in nodes:
                post_predecessor(self.address, node)

    def set_partition_member(self, node):
        """
        Adds a partition member to this node
        """
        if self.is_local():
            self.__partition_members.append(node)
        else:
            post_partition_member(self.address, node)

    def set_partition_members(self, nodes):
        """
        Adds multiple partition members to this node
        """
        if self.is_local():
            self.__partition_members.extend(nodes)
        else:
            for node in nodes:
                post_partition_member(self.address, node)

    def remove_successor(self, node):
        """
        Removes successor from this node
        """
        if self.is_local():
            for successor_node in self.__successors:
                if node.address == successor_node.address:
                    self.__successors.remove(successor_node)
        else:
            delete_successor(self.address, node)

    def remove_predecessor(self, node):
        """
        Removes predecessor from this node
        """
        if self.is_local():
            for predecessor_node in self.__predecessors:
                if node.address == predecessor_node.address:
                    self.__predecessors.remove(predecessor_node)
        else:
            delete_predecessor(self.address, node)

    def remove_partition_member(self, node):
        """
        Removes partition member from this node
        """
        if self.is_local():
            for partition_member in self.__partition_members:
                if node.address == partition_member.address:
                    self.__partition_members.remove(partition_member)
        else:
            delete_partition_member(self.address, node)

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

    # new_node may be our predecessor
    def notify(self, new_node):
        """
        Sets our predecessor to be the given node
        Checks if we have any entries that belong to our predecessor and, if so, sends them along.
        """
        if self.is_local():
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

    def __repr__(self):
        """
        Returns a string-representation of self for printing
        """
        return Node.__name__ + '(' + 'address=' + repr(self.address) + ', partition_id=' + repr(self.partition_id) + ')'


# Communication functions
def get_successors(address):
    """
    Asks the node at a given IP for its successor, returns that node.
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'successors'})
    # Res comes in as a list of dicts
    successors = res.json()
    return map(lambda params: Node(**params), successors)


def get_predecessors(address):
    """
    Asks the node at a given IP for its predecessor, returns that node.
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'predecessors'})
    # Predecessors come in as a list of dicts
    predecessors = res.json()
    return map(lambda params: Node(**params), predecessors)


def get_partition_members(address):
    """
    Asks the node at a given IP for its partition members
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'partition_members'})
    # Partition members come in as a list of dicts
    partition_members = res.json()
    return map(lambda params: Node(**params), partition_members)


def post_successor(address, node):
    """
    Tells address that its new successor is node
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'successor'},
             data={'ip_port': node.address,
                   'partition_id': node.partition_id})


def post_predecessor(address, node):
    """
    Tells address that its new predecessor is node
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'predecessor'},
             data={'ip_port': node.address,
                   'partition_id': node.partition_id})


def post_partition_member(address, node):
    """
    Tells address to add node to its partition members
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'partition_member'},
             data={'ip_port': node.address,
                   'partition_id': node.partition_id})


def delete_successor(address, node):
    """
    Tells address to remove node from its successors
    """
    req.delete('http://' + address + '/kvs',
               params={'request': 'successor'},
               data={'ip_port': node.address})


def delete_predecessor(address, node):
    """
    Tells address to remove node from its predecessors
    """
    req.delete('http://' + address + '/kvs',
               params={'request': 'predecessor'},
               data={'ip_port': node.address})


def delete_partition_member(address, node):
    """
    Tells address to remove node from its partition members
    """
    req.delete('http://' + address + '/kvs',
               params={'request': 'partition_member'},
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
