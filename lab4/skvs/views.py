from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from skvs.models import KvsEntry
import requests as req
import sys
from chord_operations import localNode, Node, invite_to_join, socket


@api_view(['GET', 'POST', 'DELETE'])
def gossip(request):
    partner_ip_port = request.data.get('ip_port')
    # compare all my keys to my partner's keys
    for key in KvsEntry.objects.all():
        url_str = 'http://' + partner_ip_port + '/kvs/' + key
        # get same key from partner
        res = req.get(url_str)
        # TODO compare vector clocks once we have it as a json field in returned data
        # TODO issue replace outdated key, if the vector clocks do not match

@api_view(['GET', 'POST', 'DELETE'])
def process_remote(request):
    """
    Handles miscellaneous remote communication.
    This includes:
    1. Getting this node's successor
    2. Getting this node's predecessor
    3. Test function
    4. Posting a new successor to this node
    5. Posting a new predecessor to this node

    This is called from ip:port/kvs
    """

    if request.method == 'GET':
        # Get our successors

        print >> sys.stderr, "GET REQUEST"

        if request.query_params.get('request') == 'successors':
            find_address = request.data.get('ip_port')
            # If data has an ip and port, return the successor of that ip and port
            # TODO: Finish this first one. Saving it for after testing
            if find_address:
                correct_successors = localNode.find_successor(find_address).address
            # Otherwise, return our successor
            else:
                correct_successors = localNode.successors()
                return Response([{'address': node.address,
                                  'partition_id': node.partition_id}
                                for node in correct_successors])

        # Get our predecessors
        elif request.query_params.get('request') == 'predecessors':
            find_address = request.data.get('ip_port')
            # If data has an ip and port, return the predecessor of that ip and port
            if find_address:
                correct_predecessor = localNode.find_predecessor(find_address).address
            # Otherwise, return our predecessor
            else:
                correct_predecessors = localNode.predecessors()
                return Response([{'address': node.address,
                                  'partition_id': node.partition_id} for node in correct_predecessors])
            return Response({'address': correct_predecessor})

        # Get our partition members
        elif request.query_params.get('request') == 'partition_members':
            partition_members = localNode.partition_members()
            return Response([{'address': node.address,
                              'partition_id': node.partition_id} for node in partition_members])

        # Use this for any arbitrary test you may want to run
        elif request.query_params.get('request') == 'test':
            node = localNode.partition_members()[1]
            print >> sys.stderr, 'Node:', node
            node.set_partition_member(Node('10.0.0.0'))
            print >> sys.stderr, node.partition_members()
            node.remove_partition_member(Node('10.0.0.0'))
            print >> sys.stderr, 'Removed', node.partition_members()
            return Response({'msg': 'success'})

    elif request.method == 'POST':
        # Add a successor
        if request.query_params.get('request') == 'successor':
            successor_ip = request.data.get('ip_port')
            if successor_ip:

                localNode.set_successor(Node(successor_ip))
                return Response({'msg': 'success'})

        # Add a predecessor
        elif request.query_params.get('request') == 'predecessor':
            predecessor_ip = request.data.get('ip_port')
            if predecessor_ip:
                localNode.set_predecessor(Node(predecessor_ip))
                return Response({'msg': 'success'})

        # Add a partition member
        elif request.query_params.get('request') == 'partition_member':
            partition_member_ip = request.data.get('ip_port')
            if partition_member_ip:
                localNode.set_partition_member(Node(partition_member_ip))
                return Response({'msg': 'success'})

        # Join another node. # TODO: Make this work
        elif request.query_params.get('request') == 'joinme':
            network_ip = request.data.get('ip_port')
            if network_ip:
                localNode.join(Node(network_ip))
                current_node = localNode.predecessor()
                while current_node.address != localNode.address:
                    current_node = current_node.predecessor()
                return Response({'msg': 'success'})

        # This alerts us to the existence of a new predecessor # TODO: Make this work
        elif request.query_params.get('request') == 'notify':
            new_ip = request.data.get('ip_port')
            if new_ip:
                localNode.notify(Node(new_ip))
                return Response({'msg': 'success'})

    elif request.method == 'DELETE':
        # Delete a successor
        if request.query_params.get('request') == 'successor':
            successor_ip = request.data.get('ip_port')
            if successor_ip:
                localNode.remove_successor(Node(successor_ip))
                return Response({'msg': 'success'})

        # Delete a predecessor
        elif request.query_params.get('request') == 'predecessor':
            predecessor_ip = request.data.get('ip_port')
            if predecessor_ip:
                localNode.remove_predecessor(Node(predecessor_ip))
                return Response({'msg': 'success'})

        elif request.query_params.get('request') == 'partition_member':
            partition_member_ip = request.data.get('ip_port')
            if partition_member_ip:
                localNode.remove_partition_member(Node(partition_member_ip))
                return Response({'msg': 'success'})

    return Response(request.data, status=status.HTTP_400_BAD_REQUEST)


# handles view change requests
@api_view(['PUT'])
def view_change(request):
    """
    Handles view changes.
    For adding or removing node, query param = type, data = ip_port
    For joining another node's circle, query param = 'joinme'
    """
    # change_type = request.query_params

    ip_port = request.data.get('ip_port')
    if ip_port:
        change_type = request.query_params.get('type')
        if change_type == 'add':
            # If add, then we signal the given IP to join us
            # try:
            res = invite_to_join(ip_port)

            return Response(res.json(), status=res.status_code)
            # except Exception:
            #     return Response(status=status.HTTP_400_BAD_REQUEST)

        elif change_type == 'remove':
            # if we are the node that is going to be removed
            if ip_port == localNode.address:

                # set the node's successor's predecessor to the node's predecessor
                localNode.successor().set_predecessor(localNode.predecessor())
                # set the node's predecessor's successor to the node's successor
                localNode.predecessor().set_successor(localNode.successor())

                # migrate the key-values
                for kvs_entry in KvsEntry.objects.all():
                    key = kvs_entry.key
                    val = kvs_entry.value

                    url_str = 'http://' + localNode.successor().address + '/kvs/' + str(key)

                    res = req.put(url_str, data={'val': val})

                    if res.status_code == status.HTTP_200_OK or res.status_code == status.HTTP_201_CREATED:
                        KvsEntry.objects.get(key=key).delete()
                    else:
                        return Response(res.json(), status=res.status_code)

                # then set doomed node predecessor and successor to None
                localNode.set_predecessor(None)
                localNode.set_successor(None)

                return Response({'msg': 'success'}, status=status.HTTP_200_OK)

            # if instead, we are not the node to be removed, forward the remove_node request to the doomed node
            else:
                res = req.put("http://" + ip_port + '/kvs/view_update',
                              params={'type': 'remove'},
                              data={'ip_port': ip_port})
                return Response(res.json(), status=res.status_code)

    return Response({'msg': 'Error: No IP_PORT'}, status=status.HTTP_400_BAD_REQUEST)


# handle incorrect keys
@api_view(['GET', 'PUT', 'DELETE'])
def bad_key_response(request, key):
    return Response(status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)


# handle correct keys
@api_view(['GET', 'PUT', 'DELETE'])
def kvs_response(request, key):
    """
    Key comes in guaranteed to be r'[a-zA-Z0-9_]' between 1 and 250 characters thanks to the magic of regex and urls.py
    """
    method = request.method

    # if we are the main instance, retrieve requested data
    if localNode.is_mine(key):

        # if PUT, attempt to retrieve associated value and input key
        if method == 'PUT':
            # Request requirements:
            #   must have 'val'
            if 'val' not in request.data:
                return Response({'msg': 'error', 'error': 'No value given'}, status=status.HTTP_400_BAD_REQUEST)
            input_value = request.data['val']
            #   must be that value > 256MB
            if sys.getsizeof(input_value) > 1024 * 1024 * 256:
                return Response({'msg': 'error', 'error': 'Size of key too big'}, status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)

            # If object with that key does not exist, it will be created. If it does exist, value will be updated.
            obj, created = KvsEntry.objects.update_or_create(key=key, defaults={'value': input_value})
            if created:
                return Response({'replaced': 0, 'msg': 'success', 'owner': localNode.address},
                                status=status.HTTP_201_CREATED)
            else:
                return Response({'replaced': 1, 'msg': 'success', 'owner': localNode.address},
                                status=status.HTTP_200_OK)

        # if GET, attempt to see if key exists, return found value or resulting error.
        elif method == 'GET':
            try:
                desired_entry = KvsEntry.objects.get(key=key)
                return Response({'msg': 'success', 'value': desired_entry.value, 'owner': localNode.address},
                                status=status.HTTP_200_OK)
            except KvsEntry.DoesNotExist:
                expectedOwner = localNode.find_successor(key)
                return Response({'msg': 'error', 'error': 'key does not exist', 'owner': expectedOwner.address}, status=status.HTTP_404_NOT_FOUND)

        # check if key exists, delete if so, otherwise return error message
        elif method == 'DELETE':
            try:
                KvsEntry.objects.get(key=key).delete()
                return Response({'msg': 'success', 'owner': localNode.address}, status=status.HTTP_200_OK)
            except KvsEntry.DoesNotExist:
                expectedOwner = localNode.find_successor(key)
                return Response({'msg': 'error', 'error': 'key does not exist', 'owner': expectedOwner.address}, status=status.HTTP_404_NOT_FOUND)

    # if it was not ours, we must forward the query to our successor
    else:

        # get successor's ip address
        successor_ip = localNode.get_successor_ip()

        # create the proper url with successor's ip
        url_str = 'http://' + successor_ip + '/kvs/' + key

        if method == 'GET':
            # forward request with query content
            res = req.get(url_str)

        elif method == 'PUT':
            # forward to main whether or not the request is empty
            res = req.put(url_str, data=request.data)

        elif method == 'DELETE':
            # forward request as delete operation
            # check if item exists, delete if so:
            res = req.delete(url_str)

        return Response(res.json(), status=res.status_code)
