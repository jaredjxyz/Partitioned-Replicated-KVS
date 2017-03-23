import requests as req
import os
import random
from time import sleep
from collections import Counter
from requests.exceptions import ConnectionError, ReadTimeout
from threading import Thread
import sys

KvsEntry = None
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


# This is initialized at Django startup (see apps.py)
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

        self.counter = Counter()
        # self.counter[self.partition_id] = 0
        self.__predecessors = []
        # initialize empty partition members
        self.__partition_members = []
        self.__partition_id = partition_id

        self.ready = None

    def id(self):
        return double_hash(self.partition_id()) % SIZE

    # determine if this key is stored on our node
    def is_mine(self, key):
        key_location = double_hash(key) % SIZE
        return in_range(key_location, self.predecessors()[0].id(), self.id())

    # query successor's ip so we can forward request
    def get_successor_ip(self):
        return self.successors()[0].address

    def is_local(self):
        return self == localNode

    def is_remote(self):
        return self != localNode

    # ## Run gossip in background randomly within an alright range of time
    def run_gossip(self, in_thread=False):
        if self.is_local():
            # If we're already in a separate thread, run gossip in this thread
            if in_thread:
                while self.partition_id() is not None:
                    # Wait between 0 and (# of partition members)*5 seconds
                    wait_time = random.random() * len(self.partition_members()) * 5

                    # Tell a random other node in our partition to ask around
                    partition_members = [node for node in self.partition_members() if node.address != self.address]

                    # If partition members is not empty, ask a partition member to gossip with me
                    if partition_members:
                        partner_node = random.choice(partition_members)
                        req.put('http://' + partner_node.address + '/kvs/gossip', data={'ip_port': self.address}, timeout=3)
                    sleep(wait_time)

            # If we're not already in a separate thread, create a new separate thread and run it
            else:
                gossipThread = Thread(target=self.run_gossip, kwargs={'in_thread': True})
                gossipThread.start()
        else:
            run_gossip(self.address)

    # Code for getting and setting successor and predecessor.
    # Use these for setting and getting, do NOT use the assignment operation on __successor or __predecessor

    def successors(self):
        """
        Returns the successor node to this node.
        If this node is a remote node, it queries the remote location and returns that location's successor.
        If this node is a local node, it just returns the known successor
        """
        if self.is_local():
            successor_list = []
            for successor in self.__successors:
                # Ping successor to make sure it's still alive
                try:
                    successor.partition_id()
                    successor_list.append(successor)
                except (ConnectionError, ReadTimeout):
                    self.__successors.remove(successor)
            return successor_list
        else:
            return get_successors(self.address)

    def predecessors(self):
        """
        Returns the predecessor node to this node.
        If this node is a remote node, it queries the remote location and returns that location's predecessor.
        If this node is a local node, it just returns the known predecessor.
        """
        if self.is_local():
            predecessor_list = []
            for predecessor in self.__predecessors:
                try:
                    predecessor.partition_id()
                    predecessor_list.append(predecessor)
                except (ConnectionError, ReadTimeout):
                    self.__predecessors.remove(predecessor)
            return predecessor_list
        else:
            return get_predecessors(self.address)

    def partition_members(self):
        """
        Returns a list of all members of this partition, including self
        """
        if self.is_local():
            partition_member_list = []
            for partition_member in self.__partition_members:
                try:
                    partition_member.partition_id()
                    partition_member_list.append(partition_member)
                except (ConnectionError, ReadTimeout):
                    self.__partition_members.remove(partition_member)
            return list(self.__partition_members)
        else:
            return get_partition_members(self.address)

    def partition_id(self):
        """
        Returns this node's partition ID, contacts the actual node if necessary
        """
        if self.is_local():
            return self.__partition_id
        else:
            return get_partition_id(self.address)

    def set_successor(self, node):
        """
        Sets the successor node of this node, given and address
        If this node is a remote node it queries the remote location and tells it to set its successor
        If this node is a local node, it just sets the local successor.
        """
        if self.is_local():
            if not any(node.address == successor.address for successor in self.__successors):
                self.__successors.append(node)
        else:
            post_successor(self.address, node)

    def set_successors(self, nodes):
        """
        Sets multiple successors
        """
        if self.is_local():
            for node in nodes:
                self.set_successor(node)
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
            if not any(node.address == predecessor.address for predecessor in self.__predecessors):
                self.__predecessors.append(node)
        else:
            post_predecessor(self.address, node)

    def set_predecessors(self, nodes):
        """
        Sets multiple predecessors of this node
        """
        if self.is_local():
            for node in nodes:
                self.set_predecessor(node)
        else:
            for node in nodes:
                post_predecessor(self.address, node)

    def set_partition_member(self, node):
        """
        Adds a partition member to this node
        """

        if self.is_local():
            if not any(node.address == partition_member.address for partition_member in self.__partition_members):
                self.__partition_members.append(node)
        else:
            post_partition_member(self.address, node)

    def set_partition_members(self, nodes):
        """
        Adds multiple partition members to this node
        """
        if self.is_local():
            for node in nodes:
                self.set_partition_member(node)
        else:
            for node in nodes:
                post_partition_member(self.address, node)

    def set_partition_id(self, partition_id):
        """
        Sets this node's partition ID
        """

        if self.is_local():
            self.__partition_id = partition_id
        else:
            send_partition_id(self.address, partition_id)

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
    def find_successors(self, key):
        current_partition = self.partition_members()
        found = False
        while not found:
            for node in current_partition:
                try:
                    if not node.is_mine(key):
                        current_partition = node.successors()
                        break
                    else:
                        found = True
                        break
                except (ConnectionError, ReadTimeout):
                    continue
        return current_partition

    # have this Node find the predecessor of a given slot
    def find_predecessor(self, key):
        if self.is_local():
            desired_node = self
            while not desired_node.is_mine(key):
                desired_node = desired_node.successor()
            return desired_node.predecessor()
        else:
            res = req.get('http://' + self.address + '/kvs', params={'request': 'predecessor'}, data={'ip_port': key}, timeout=3)
            # res comes in as {'address': ip_port}
            return Node(**res.json())

    # return closest node that precedes the id
    # NOTE: For now, does the same thing as find_predecessor. This will change when implenting chord
    def closest_preceding_node(self, key):
        return self.find_predecessor(key)

    # ######## Code for joining the distributed hash table ######## #

    def join(self, new_node):
        # Find our successors and keep finding their successors until there's a not full group
        group_numbers_and_sizes = {self.partition_id(): (len(self.partition_members()), self)}
        current_group = self.successors()

        # Get a list of partition numbers and their sizes and their representative nodes
        while not any(self.address == node.address for node in current_group):  # While I'm not in the successors list
            for node in current_group:
                try:  # Try all nodes in group till one responds
                    group_size = len(node.partition_members())
                    group_number = node.partition_id()
                    group_numbers_and_sizes[group_number] = (group_size, node)
                    current_group = node.successors()
                    break
                except (ConnectionError, ReadTimeout):  # They're down
                    continue

        # Find the group the new node should go in
        smallest_group = min(group_numbers_and_sizes, key=lambda x: group_numbers_and_sizes[x])
        smallest_group_size, smallest_group_representative = group_numbers_and_sizes[smallest_group]

        # If there is a group with an empty spot, go there
        if smallest_group_size < int(os.environ['K']):

            new_node.set_partition_id(smallest_group)

            for partition_member in smallest_group_representative.partition_members():
                partition_member.set_partition_member(new_node)
                new_node.set_partition_member(partition_member)

            for successor in smallest_group_representative.successors():
                successor.set_predecessor(new_node)
                new_node.set_successor(successor)

            for predecessor in smallest_group_representative.predecessors():
                predecessor.set_successor(new_node)
                new_node.set_predecessor(predecessor)

            new_node.set_partition_member(new_node)

        # Create our own new partition
        else:
            new_partition_number = max(group_numbers_and_sizes) + 1
            new_node.set_partition_id(new_partition_number)

            # Find where we belong
            for group_size, node in group_numbers_and_sizes.values():
                if node.is_mine(new_partition_number):
                    my_successors = list(node.partition_members())
                    my_predecessors = list(node.predecessors())
                    break

            for successor in my_successors:
                for predecessor in my_predecessors:
                    successor.remove_predecessor(predecessor)
                successor.set_predecessor(new_node)

            for predecessor in my_predecessors:
                for successor in my_successors:
                    predecessor.remove_successor(successor)
                predecessor.set_successor(new_node)

            new_node.set_successors(my_successors)
            new_node.set_predecessors(my_predecessors)
            new_node.set_partition_member(new_node)

        # Stick notify here
        self.notify(new_node)
        new_node.run_gossip()

    # new_node may be our predecessor
    # TODO: Test notify
    def notify(self, new_node):
        """
        Sets our predecessor to be the given node
        Checks if we have any entries that belong to our predecessor and, if so, sends them along.
        """
        if self.is_local():
            predecessor = list(self.predecessors())[0]
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
        return Node.__name__ + '(' + 'address=' + repr(self.address) + ')'


# Communication functions
def get_successors(address):
    """
    Asks the node at a given IP for its successor, returns that node.
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'successors'}, timeout=3)
    # Res comes in as a list of dicts
    successors = res.json()
    return map(lambda params: Node(**params), successors)


def get_predecessors(address):
    """
    Asks the node at a given IP for its predecessor, returns that node.
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'predecessors'}, timeout=3)
    # Predecessors come in as a list of dicts
    predecessors = res.json()
    return map(lambda params: Node(**params), predecessors)


def get_partition_members(address):
    """
    Asks the node at a given IP for its partition members
    """
    res = req.get('http://' + address + '/kvs', params={'request': 'partition_members'}, timeout=3)
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
                   'partition_id': node.partition_id()}, timeout=3)


def post_predecessor(address, node):
    """
    Tells address that its new predecessor is node
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'predecessor'},
             data={'ip_port': node.address,
                   'partition_id': node.partition_id()}, timeout=3)


def post_partition_member(address, node):
    """
    Tells address to add node to its partition members
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'partition_member'},
             data={'ip_port': node.address,
                   'partition_id': node.partition_id()}, timeout=3)


def delete_successor(address, node):
    """
    Tells address to remove node from its successors
    """
    req.delete('http://' + address + '/kvs',
               params={'request': 'successor'},
               data={'ip_port': node.address}, timeout=3)


def delete_predecessor(address, node):
    """
    Tells address to remove node from its predecessors
    """
    req.delete('http://' + address + '/kvs',
               params={'request': 'predecessor'},
               data={'ip_port': node.address}, timeout=3)


def delete_partition_member(address, node):
    """
    Tells address to remove node from its partition members
    """
    req.delete('http://' + address + '/kvs',
               params={'request': 'partition_member'},
               data={'ip_port': node.address}, timeout=3)


def get_partition_id(address):
    """
    Asks the address for its partition ID
    """
    res = req.get('http://' + address + '/kvs',
                  params={'request': 'partition_id'}, timeout=.5)

    return res.json()['partition_id']


def send_partition_id(address, partition_id):
    """
    Tells address to set its partition id to partition_id
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'partition_id'},
             data={'id': partition_id}, timeout=3)


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
             data={'ip_port': node.address}, timeout=3)


def run_gossip(address):
    """
    Tells address to start gossipping
    """
    req.post('http://' + address + '/kvs',
             params={'request': 'run_gossip'}, timeout=3)


def ask_ready(address):
    """
    Asks address if it's ready
    """
    try:
        res = req.get('http://' + address + '/kvs',
                      params={'request': 'ready'}, timeout=3)

    except (ConnectionError, ReadTimeout):
        return None
    return res.json()['msg']
