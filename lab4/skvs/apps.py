from __future__ import unicode_literals

from django.apps import AppConfig
import chord_operations
import os

class SkvsConfig(AppConfig):
    name = 'skvs'

    def ready(self):
        #Import kvsentry into chord operations
        from models import KvsEntry
        chord_operations.KvsEntry = KvsEntry

        myIP = os.environ.get('IPPORT')

        # Checks for the case when Django's second thread starts up
        # We have to do this because of how Django's auto-load works
        # Put initialization stuff in this if statement
        if myIP is not None:
            # Check to see if environment variable VIEW is set up.
            # If it is, this is the first time through. Find my predecessor and successor.
            chord_operations.localNode = chord_operations.Node(myIP)
            if 'VIEW' in os.environ:
                addresses = [x.strip() for x in os.environ['VIEW'].split(',')]
                addresses.remove(myIP)

                myID = hash(myIP)
                successor_IP = myIP
                predecessor_IP = myIP
                # If we're by ourselves, we're our own successor and predecessor.
                # Otherwise, find our true successor and predecessor
                while addresses:
                    potential_address = addresses.pop()

                    # Keep checking new addresses. If address is in range, it is our new closest successor/predecessor
                    if chord_operations.in_range(hash(potential_address), myID, hash(successor_IP)):
                        successor_IP = potential_address

                    if chord_operations.in_range(hash(potential_address), hash(predecessor_IP), myID):
                        predecessor_IP = potential_address

                chord_operations.localNode.set_successor(chord_operations.Node(successor_IP))
                chord_operations.localNode.set_predecessor(chord_operations.Node(predecessor_IP))
