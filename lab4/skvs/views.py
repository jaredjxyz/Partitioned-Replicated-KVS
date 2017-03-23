from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from skvs.models import KvsEntry
import requests as req
import sys
import time
import random
from collections import \
    Counter  # Don't listen to the linter he LIES and tells you we're not using this. Bad linter. No.
from chord_operations import localNode, Node
from requests.exceptions import ConnectionError, ReadTimeout


@api_view(['GET'])
def get_partition_id(request):
        return Response({'msg': 'success',
                        'partition_id': localNode.partition_id()})


@api_view(['GET'])
def get_partition_members(request):
    requested_id = int(request.data.get('partition_id'))

    if 'source' in request.data and int(request.data.get('source')) == localNode.partition_id():
            return Response({'msg': 'error', 'error': 'partition id does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

    if localNode.partition_id() == requested_id:
        members = [node.address for node in localNode.partition_members()]
        return Response({'msg': 'success',
                         'partition_members': members}, status=status.HTTP_200_OK)
    else:
        successor_ip = localNode.get_successor_ip()
        res = req.get('http://' + successor_ip + '/kvs/get_partition_members',
                      data={'partition_id': requested_id, 'source': int(localNode.partition_id())}, timeout=3)

    return Response(res.json(), status=res.status_code)


@api_view(['PUT'])
def gossip(request):
    partner_ip_port = request.data.get('ip_port')

    # Put the partition member back if he's not in our successors
    if not any(partition_member.address == partner_ip_port for partition_member in localNode.partition_members()):
        partner_node = Node(partner_ip_port)
        successors = localNode.successors()
        predecessors = localNode.predecessors()
        partition_members = localNode.partition_members()

        for successor in successors:
            successor.set_predecessor(partner_node)
        for predecessor in predecessors:
            predecessor.set_successors(partner_node)
        for partition_member in partition_members:
            partition_member.set_partition_member(partner_node)

    # compare all my keys to my partner's keys

    for entry in KvsEntry.objects.all():
        url_str = 'http://' + partner_ip_port + '/kvs/' + entry.key

        # get same key from partner
        res_data = req.get(url_str, timeout=3).json()
        partner_vc = eval(res_data['causal_payload'])
        my_vc = eval(entry.clock)

        # if our vector clocks are not equal
        if my_vc[localNode.partition_id()] != partner_vc[localNode.partition_id()]:
            temp = localNode.counter
            # Update counter with partner vc. Counter should already be up to date with our vc
            localNode.counter |= partner_vc
            # update our key if it is the stale one
            if partner_vc[localNode.partition_id()] > temp[localNode.partition_id()]:
                KvsEntry.objects.update_or_create(key=entry.key, defaults={'value': res_data['value'],
                                                                           'timestamp': res_data['timestamp'],
                                                                           'clock': repr(
                                                                               localNode.counter)})
            # TODO tiebreaker based on server id + timestamp
            # elif : partner wins the tiebreak
            elif partner_ip_port + str(res_data['timestamp']) > localNode.address + str(
                    KvsEntry.objects.get(key=entry.key).timestamp):
                # Update our local VC
                localNode.counter |= partner_vc

                KvsEntry.objects.update_or_create(key=entry.key, defaults={'value': res_data['value'],
                                                                           'timestamp': res_data['timestamp'],
                                                                           'clock': repr(
                                                                               localNode.counter)})
        # if vector clocks are equal, but the stored times are different, tiebreak
        if (my_vc[localNode.partition_id()] == partner_vc[localNode.partition_id()] and
           entry.timestamp < res_data['timestamp']):
            localNode.counter += partner_vc
            # if we are the stale value, update our key to partner's key
            KvsEntry.objects.update_or_create(key=entry.key, defaults={'value': res_data['value'],
                                                                       'timestamp': res_data['timestamp'],
                                                                       'clock': repr(
                                                                       localNode.counter)})
    return Response({'msg': 'OK'})


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
                                  'partition_id': node.partition_id()}
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
                                  'partition_id': node.partition_id()} for node in correct_predecessors])
            return Response({'address': correct_predecessor})

        elif request.query_params.get('request') == 'partition_id':
            partition_id = localNode.partition_id()
            return Response({'partition_id': partition_id})

        # Get our partition members
        elif request.query_params.get('request') == 'partition_members':
            partition_members = localNode.partition_members()
            return Response([{'address': node.address,
                              'partition_id': node.partition_id()} for node in partition_members])

        elif request.query_params.get('request') == 'ready':
            return Response({'msg': localNode.ready})

        # Use this for any arbitrary test you may want to run
        elif request.query_params.get('request') == 'test':
            node = localNode.partition_members()[1]
            node.set_partition_member(Node('10.0.0.0'))
            node.remove_partition_member(Node('10.0.0.0'))
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
            if partition_member_ip is not None:
                localNode.set_partition_member(Node(partition_member_ip))
                return Response({'msg': 'success'})

        elif request.query_params.get('request') == 'partition_id':
            partition_id = request.data.get('id')
            if partition_id:
                localNode.set_partition_id(int(partition_id))
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

        elif request.query_params.get('request') == 'run_gossip':
            localNode.run_gossip()
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

    return Response({'msg', 'Invalid Request'}, status=status.HTTP_400_BAD_REQUEST)


# handles view change requests
@api_view(['PUT'])
def view_change(request):
    """
    Handles view changes.
    For adding or removing node, query param = type, data = ip_port
    For joining another node's circle, query param = 'joinme'
    """

    ip_port = request.data.get('ip_port')
    if ip_port:
        change_type = request.query_params.get('type')
        if change_type == 'add':
            # If add, then we signal the given IP to join us
            # try:
            new_node = Node(ip_port)
            localNode.join(new_node)

            # Get a list of partition Ids so we can return the length
            partition_id_res = req.get('http://' + localNode.address + '/kvs/get_all_partition_ids', timeout=3)
            partition_id_list = eval(partition_id_res.json()['partition_id_list'])
            num_partition_ids = len(partition_id_list)
            return Response({'msg': 'success',
                             'partition_id': new_node.partition_id(),
                             'number_of_partitions': num_partition_ids})

        # TODO: Test Remove!
        elif change_type == 'remove':
            # if we are the node that is going to be removed
            if ip_port == localNode.address:

                # Get partition Ids for response
                partition_id_res = req.get('http://' + localNode.address + '/kvs/get_all_partition_ids', timeout=3)
                partition_id_list = eval(partition_id_res.json()['partition_id_list'])
                num_partition_ids = len(partition_id_list)

                successors = localNode.successors()
                predecessors = localNode.predecessors()
                partition_members = localNode.partition_members()
                # check if we will still have others in our partition after our deletion
                if len(localNode.partition_members()) > 1:
                    for partition_member in partition_members:
                        partition_member.remove_partition_member(localNode)
                        localNode.remove_partition_member(partition_member)

                    for successor in successors:
                        successor.remove_predecessor(localNode)
                        localNode.remove_successor(successor)

                    for predecessor in predecessors:
                        predecessor.remove_successor(localNode)
                        localNode.remove_predecessor(predecessor)

                    # if so, then delete all our entries after performing gossip
                    partner_node = successors[0]
                    req.put('http://' + partner_node.address + '/kvs/gossip', params={'request': 'gossip'},
                            data={'ip_port': localNode.address}, timeout=3)

                    for entry in KvsEntry.objects.all():
                        entry.delete()

                else:
                    # otherwise, we must alter succ/pred relationships, then migrate our keys
                    # set this node's successors' predecessors to the node's predecessors
                    for succ_node in successors:
                        succ_node.remove_predecessor(localNode)
                        localNode.remove_successor(succ_node)
                        for pred_node in predecessors:
                            succ_node.set_predecessor(pred_node)

                    # set the node's predecessors' successors to the node's successors
                    for pred_node in predecessors:
                        pred_node.remove_successor(localNode)
                        localNode.remove_predecessor(pred_node)
                        for succ_node in successors:
                            pred_node.set_successor(succ_node)

                    for partition_member in partition_members:
                        partition_member.remove_partition_member(localNode)

                    print >> sys.stderr, "Getting to kvs entries"
                    # migrate the key-values
                    for kvs_entry in KvsEntry.objects.all():
                        key = kvs_entry.key
                        val = kvs_entry.value

                        successor = random.choice(successors)

                        url_str = 'http://' + successor.address + '/kvs/' + str(key)

                        res = req.put(url_str, data={'val': val,
                                                     'causal_payload': kvs_entry.clock,
                                                     'timestamp': kvs_entry.timestamp}, timeout=3)

                        if not (res.status_code == status.HTTP_200_OK or res.status_code == status.HTTP_201_CREATED):
                            return Response(res.json(), status=res.status_code)

                    # then set doomed node predecessor and successor to None
                    num_partition_ids -= 1

                localNode.set_partition_id(None)

                return Response({'msg': 'success',
                                 'number_of_partitions': num_partition_ids})

            # if instead, we are not the node to be removed, forward the remove_node request to the doomed node
            else:
                try:
                    res = req.put("http://" + ip_port + '/kvs/update_view',
                                  params={'type': 'remove'},
                                  data={'ip_port': ip_port}, timeout=3)
                except (ConnectionError, ReadTimeout):
                    partition_id_res = req.get('http://' + localNode.address + '/kvs/get_all_partition_ids', timeout=3)
                    partition_id_list = eval(partition_id_res.json()['partition_id_list'])
                    num_partition_ids = len(partition_id_list)
                    return Response({'msg': 'success',
                                     'number_of_partitions': num_partition_ids})

    return Response({'msg': 'Error: Bad Request'}, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def get_all_partition_ids(request):

    successor_ip = localNode.get_successor_ip()
    url_str = 'http://' + successor_ip + '/kvs/get_all_partition_ids'

    if 'source' not in request.data:
        list = []
        list.extend([int(localNode.partition_id())])
        res = req.get(url_str, data={'source': localNode.partition_id(), 'partition_id_list': repr(list)}, timeout=3)

    if 'source' in request.data and int(request.data['source']) == localNode.partition_id():
        return Response({'msg': 'success', 'partition_id_list': request.data['partition_id_list']})

    if 'source' in request.data and int(request.data['source']) != localNode.partition_id():
        list = eval(request.data['partition_id_list'])
        list.extend([int(localNode.partition_id())])
        res = req.get(url_str, data={'source': request.data['source'], 'partition_id_list': repr(list)}, timeout=3)

    return Response(res.json(), status=status.HTTP_200_OK)


@api_view(['PUT'])
def payload_update(request):
    localNode.counter = localNode.counter | eval(request.data['load'])
    return Response(status=status.HTTP_200_OK)


# handle incorrect keys
@api_view(['GET', 'PUT', 'DELETE'])
def bad_key_response(request, key):
    return Response(status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)


# tell your friends
@api_view(['PUT'])
def broadcast_put(request):
    merge = request.data
    localNode.counter = localNode.counter | eval(merge['clock'])
    KvsEntry.objects.update_or_create(key=merge['key'], defaults={'value': merge['value'], 'timestamp': merge['timestamp'],
                                                                  'clock': repr(localNode.counter)})
    return Response(status=status.HTTP_200_OK)


@api_view(['GET'])
def get_simple(request):
    try:
        desired_entry = KvsEntry.objects.get(key=request.data['key'])
        return Response(
            {'msg': 'success', 'key': desired_entry.key, 'value': desired_entry.value, 'source': 'get_simple',
             'owner': localNode.address, 'timestamp': desired_entry.timestamp, 'clock': desired_entry.clock},
            status=status.HTTP_200_OK)
    except Exception:
        expectedOwner = localNode.find_successors(request.data['key'])[0]
        return Response(
            {'msg': 'error', 'error': 'key does not exist', 'source': 'get_simple', 'owner': expectedOwner.address},
            status=status.HTTP_404_NOT_FOUND)


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
                return Response({'msg': 'error', 'error': 'Size of key too big'},
                                status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)

            # get timestamp and counter
            t = float("{0:.2f}".format(time.time()))  # jared magic
            localNode.counter[localNode.partition_id()] += 1

            # If object with that key does not exist, it will be created. If it does exist, value will be updated.
            obj, created = KvsEntry.objects.update_or_create(key=key, defaults={'value': input_value, 'timestamp': t,
                                                                                'clock': repr(localNode.counter)})
            if created:
                for node in localNode.partition_members():
                    try:
                        req.put('http://' + node.address + '/broadcast_put',
                                data={'key': key, 'value': input_value, 'timestamp': t, 'clock': repr(localNode.counter)}, timeout=3)
                    except (ConnectionError, ReadTimeout):
                        pass
                return Response({'replaced': 0, 'msg': 'success', 'partition_id': localNode.partition_id(), 'timestamp': t,
                                 'causal_payload': repr(localNode.counter)},
                                status=status.HTTP_201_CREATED)
            else:
                return Response({'replaced': 1, 'msg': 'success', 'partition_id': localNode.partition_id(), 'timestamp': t,
                                 'causal_payload': repr(localNode.counter)},
                                status=status.HTTP_200_OK)

        # if GET, attempt to see if key exists, return found value or resulting error.
        elif method == 'GET':

            # read repair for gets since you're polling everyone anyways
            for node in localNode.partition_members():
                try:
                    cheq = req.get('http://' + node.address + '/get_simple', data={'key': key}, timeout=3)
                    if 'clock' in cheq.json():

                        # if the poll returns a vector clock sooner than our own
                        if (eval(cheq.json()['clock'])[localNode.partition_id()] >
                                localNode.counter[localNode.partition_id()]):
                            # merge clocks together
                            localNode.counter = localNode.counter | eval(cheq.json()['clock'])
                            # update keys to value with more recent clock
                            KvsEntry.objects.update_or_create(key=key, defaults={'value': cheq.json()['value'],
                                                                                 'timestamp': cheq.json()['timestamp'],
                                                                                 'clock': cheq.json()['clock']})

                        if (eval(cheq.json()['clock'])[localNode.partition_id()] ==
                           localNode.counter[localNode.partition_id()] and not
                           KvsEntry.objects.get(key=key).key == cheq.json()['key']):

                            # tiebreak and fix if they win tiebreak
                            if str(node.address) + (cheq.json()['timestamp']) > str(localNode.address) + str(
                                    KvsEntry.objects.get(key=key).timestamp):
                                KvsEntry.objects.update_or_create(key=key, defaults={'value': cheq.json()['value'],
                                                                                     'timestamp': cheq.json()['timestamp'],
                                                                                     'clock': cheq.json()['clock']})

                except (ConnectionError, ReadTimeout):
                    continue

            try:
                desired_entry = KvsEntry.objects.get(key=key)
                return Response(
                    {'msg': 'success', 'value': desired_entry.value, 'partition_id': localNode.partition_id(),
                     'timestamp': desired_entry.timestamp, 'causal_payload': desired_entry.clock},
                    status=status.HTTP_200_OK)
            except KvsEntry.DoesNotExist:
                expectedOwner = localNode.find_successors(key)[0]
                return Response(
                    {'msg': 'error', 'error': 'key does not exist', 'partition_id': expectedOwner.partition_id()},
                    status=status.HTTP_404_NOT_FOUND)

    # if it was not ours, we must forward the query to our successor
    else:

        # get successor's ip address
        successor_ip = localNode.get_successor_ip()

        # create the proper url with successor's ip
        url_str = 'http://' + successor_ip + '/kvs/' + key
        req.put('http://' + successor_ip + '/payload', data={'load': repr(localNode.counter)}, timeout=3)

        if method == 'GET':
            # forward request with query content
            res = req.get(url_str, timeout=3)

        elif method == 'PUT':
            # forward to main whether or not the request is empty
            res = req.put(url_str, data=request.data, timeout=3)

        return Response(res.json(), status=res.status_code)
