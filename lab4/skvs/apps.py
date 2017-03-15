from __future__ import unicode_literals

from django.apps import AppConfig
import chord_operations
import os
import math
import random
from chord_operations import double_hash, Node

class SkvsConfig(AppConfig):
    name = 'skvs'

    def ready(self):
        # Import kvsentry into chord operations
        from models import KvsEntry
        chord_operations.KvsEntry = KvsEntry

        myIP = os.environ.get('IPPORT')

        # Checks for the case when Django's second thread starts up
        # We have to do this because of how Django's auto-load works
        # Put initialization stuff in this if statement
        if myIP is not None:
            # Check to see if environment variable VIEW is set up.
            # If it is, this is the first time through. Find my predecessor and successor.
            chord_operations.localNode = Node(myIP)
            if 'VIEW' in os.environ and 'K' in os.environ:

                addresses = [x.strip() for x in os.environ['VIEW'].split(',')]

                partition_size = os.environ['K']
                num_partitions = math.ceil(len(addresses) / partition_size)

                # Create empty buckets for each partition and fill them up
                partitions = [[] * num_partitions]
                for i, address in enumerate(sorted(addresses)):
                    partitions[i % num_partitions].append(address)
                    # If we find our address, we know our partition number
                    if address == myIP:
                        my_partition_number = i

                # Randomize the order so that one node doesn't get used more than another
                for partition in partitions:
                    random.shuffle(partition)

                # If we're by ourselves, we're our own successor and predecessor.
                # Otherwise, find our true successor and predecessor
                successor_partition_number = my_partition_number
                predecessor_partition_number = my_partition_number

                for partition_number in len(partitions):

                    # Keep checking new addresses. If address is in range, it is our new closest successor/predecessor
                    if chord_operations.in_range(double_hash(partition_number), double_hash(my_partition_number), double_hash(successor_partition_number)):
                        successor_partition_number = partition_number

                    if chord_operations.in_range(double_hash(partition_number), double_hash(predecessor_partition_number), chord_operations.double_hash(partition_number)):
                        predecessor_partition_number = partition_number

                chord_operations.localNode.set_successors(map(lambda x: Node(x, my_partition_number), partitions[successor_partition_number]))
                chord_operations.localNode.set_predecessors(map(lambda x: Node(x, my_partition_number)), partitions[predecessor_partition_number])
