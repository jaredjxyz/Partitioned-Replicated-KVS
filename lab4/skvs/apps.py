from __future__ import unicode_literals

from django.apps import AppConfig
import chord_operations
import os
import math
import random
import sys
from time import sleep
from chord_operations import double_hash, Node
from threading import Thread


class SkvsConfig(AppConfig):
    name = 'skvs'

    def ready(self):
        print >> sys.stderr, "Getting Django ready!"
        # Import kvsentry into chord operations
        from models import KvsEntry
        chord_operations.KvsEntry = KvsEntry

        myIP = os.environ.get('IPPORT')
        print >> sys.stderr, "This is my IP: ", myIP

        # Checks for the case when Django's second thread starts up
        # We have to do this because of how Django's auto-load works
        # Put initialization stuff in this if statement
        if myIP is not None:
            # Check to see if environment variable VIEW is set up.
            # If it is, this is the first time through. Find my predecessor and successor.
            chord_operations.localNode = Node(myIP)
            if 'VIEW' in os.environ and 'K' in os.environ:

                addresses = [x.strip() for x in os.environ['VIEW'].split(',')]

                partition_size = int(os.environ['K'])
                num_partitions = int(math.ceil(float(len(addresses)) / partition_size))

                print >> sys.stderr, "Number of partitions is: ", num_partitions

                # Create empty buckets for each partition and fill them up
                partitions = [[] for i in range(num_partitions)]
                partition_number = 0

                for address in sorted(addresses):
                    partitions[partition_number].append(address)

                    # If we find our address, we know our partition number
                    if address == myIP:
                        my_partition_number = partition_number

                    # If the current partition is full, increment the partition_number
                    if len(partitions[partition_number]) == partition_size:
                        partition_number += 1

                # Randomize the order so that one node doesn't get used more than another
                for partition in partitions:
                    random.shuffle(partition)

                # If we're by ourselves, we're our own successor and predecessor.
                # Otherwise, find our true successor and predecessor
                successor_partition_number = my_partition_number
                predecessor_partition_number = my_partition_number

                for partition_number in range(len(partitions)):
                    if partition_number == my_partition_number:
                        continue

                    # Keep checking new addresses. If address is in range, it is our new closest successor/predecessor
                    if chord_operations.in_range(double_hash(partition_number), double_hash(my_partition_number), double_hash(successor_partition_number)):
                        successor_partition_number = partition_number

                    if chord_operations.in_range(double_hash(partition_number), double_hash(predecessor_partition_number), double_hash(my_partition_number)):
                        predecessor_partition_number = partition_number

                print >> sys.stderr, "Successors:", partitions[successor_partition_number]
                print >> sys.stderr, "Predecessors:", partitions[predecessor_partition_number]
                print >> sys.stderr, "My Partition:", partitions[my_partition_number]

                chord_operations.localNode.set_partition_id(my_partition_number)
                chord_operations.localNode.set_successors(map(lambda x: Node(x, successor_partition_number), partitions[successor_partition_number]))
                chord_operations.localNode.set_predecessors(map(lambda x: Node(x, predecessor_partition_number), partitions[predecessor_partition_number]))
                chord_operations.localNode.set_partition_members(map(lambda x: Node(x, my_partition_number), partitions[my_partition_number]))

                chord_operations.localNode.ready = False
                readyThread = Thread(target=getReady, args=[addresses])
                readyThread.start()

def getReady(addresses):
    while True:
        sleep(1)
        readyAddresses = [chord_operations.ask_ready(address) is not None for address in addresses]
        if all(readyAddresses):
            chord_operations.localNode.ready = True
            chord_operations.localNode.run_gossip()
            return
