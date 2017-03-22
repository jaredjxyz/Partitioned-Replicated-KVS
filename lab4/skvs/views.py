from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from skvs.models import KvsEntry
import requests as req
import sys
import time
from collections import \
    Counter  # Don't listen to the linter he LIES and tells you we're not using this. Bad linter. No.
from chord_operations import localNode, Node


@api_view(['GET'])
def get_partition_id(request):
        return Response({'msg': 'success',
                        'partition_id': localNode.partition_id()})


@api_view(['GET'])
def get_partition_members(request):
    requested_id = int(request.data.get('partition_id'))
    if requested_id is not None:

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
                          data={'partition_id': requested_id, 'source': int(localNode.partition_id())})

        return Response(res.json(), status=res.status_code)

    else:
        return Response({'msg': 'error', 'error': 'invalid partition id'},
                        status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET', 'POST', 'DELETE'])
def gossip(request):
    partner_ip_port = request.data.get('ip_port')
    # compare all my keys to my partner's keys
    for entry in KvsEntry.objects.all():
        url_str = 'http://' + partner_ip_port + '/kvs/' + entry.key

        # get same key from partner
        partner_vc = eval(req.get(url_str).json()['causal_payload'])
        # if our vector clocks are not equal
        if not eval(entry.clock)[localNode.partition_id()] == partner_vc[localNode.partition_id()]:
            temp = localNode.counter
            localNode.counter = localNode.counter | partner_vc
            # update our key if it is the stale one
            if partner_vc[localNode.partition_id()] > temp[localNode.partition_id()]:
                KvsEntry.objects.update_or_create(key=entry.key, defaults={'value': req.get(url_str).json()['value'],
                                                                           'time': req.get(url_str).json()['time'],
                                                                           'clock': repr(
                                                                               localNode.counter | partner_vc)})
            # TODO tiebreaker based on server id + timestamp
            # elif : partner wins the tiebreak
            elif str(partner_ip_port) + str(req.get(url_str).json()['time']) > str(localNode.address) + str(
                    KvsEntry.objects.get(key=entry.key).time):
                KvsEntry.objects.update_or_create(key=entry.key, defaults={'value': req.get(url_str).json()['value'],
                                                                           'time': req.get(url_str).json()['time'],
                                                                           'clock': repr(
                                                                               localNode.counter | partner_vc)})
        # if vector clocks are equal, but the stored times are different, tiebreak
        if eval(entry.clock)[localNode.partition_id()] == partner_vc[localNode.partition_id()] and not entry.time == \
                req.get(url_str).json()['time']:
            # if we are the stale value, update our key to partner's key
            if entry.time < req.get(url_str).json()['time']:
                KvsEntry.objects.update_or_create(key=entry.key, defaults={'value': req.get(url_str).json()['value'],
                                                                           'time': req.get(url_str).json()['time'],
                                                                           'clock': repr(
                                                                               localNode.counter | partner_vc)})


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

        elif request.query_params.get('request') == 'partition_id':
            partition_id = request.data.get('id')
            if partition_id:
                localNode.set_partition_id(partition_id)
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

            ip = localNode.address
            url_str = 'http://' + ip + '/get_all_partition_ids'

            return Response({'msg': 'success', 'partition_id': new_node.partition_id(),
                            'number_of_partitions': len(eval(req.get(url_str).json()['partition_id_list']))},
                            status=status.HTTP_201_CREATED)
            # except Exception:
            #     return Response(status=status.HTTP_400_BAD_REQUEST)

        # TODO: Test Remove!
        elif change_type == 'remove':
            # if we are the node that is going to be removed
            if ip_port == localNode.address:
                # check if we will still have others in our partition after our deletion
                if localNode.partition_members() > 1:
                    # if so, then delete all our entries after performing gossip
                    partner_node = localNode.successors()[0].address
                    req.put('http://' + partner_node + '/kvs/gossip', params={'request': 'gossip'},
                            data={'ip_port': localNode.address})
                    for entry in KvsEntry.objects.all():
                        KvsEntry.objects.get(key=entry.key).delete()
                else:
                    # otherwise, we must alter succ/pred relationships, then migrate our keys
                    # set this node's successors' predecessors to the node's predecessors
                    for node in localNode.successors():
                        for pred_node in localNode.predecessors():
                            node.set_predecessor(pred_node)

                    # set the node's predecessors' successors to the node's successors
                    for pred_node in localNode.predecessors():
                        for succ_node in localNode.successors():
                            pred_node.set_successor(succ_node)

                    # migrate the key-values
                    for kvs_entry in KvsEntry.objects.all():
                        key = kvs_entry.key
                        val = kvs_entry.value

                        url_str = 'http://' + localNode.successors()[0].address + '/kvs/' + str(key)

                        res = req.put(url_str, data={'val': val})

                        if res.status_code == status.HTTP_200_OK or res.status_code == status.HTTP_201_CREATED:
                            pass
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


@api_view(['GET'])
def get_all_partition_ids(request):

    successor_ip = localNode.get_successor_ip()
    url_str = 'http://' + successor_ip + '/kvs/get_all_partition_ids'

    if 'source' not in request.data:
        list = []
        list.extend([int(localNode.partition_id())])
        res = req.get(url_str, data={'source': localNode.partition_id(), 'partition_id_list': repr(list)})

    if 'source' in request.data and int(request.data['source']) == localNode.partition_id():
        return Response({'msg': 'success', 'partition_id_list': request.data['partition_id_list']})

    if 'source' in request.data and int(request.data['source']) != localNode.partition_id():
        list = eval(request.data['partition_id_list'])
        list.extend([int(localNode.partition_id())])
        res = req.get(url_str, data={'source': request.data['source'], 'partition_id_list': repr(list)})

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
    KvsEntry.objects.update_or_create(key=merge['key'], defaults={'value': merge['value'], 'time': merge['time'],
                                                                  'clock': repr(localNode.counter)})
    return Response(status=status.HTTP_200_OK)


@api_view(['GET'])
def get_simple(request):
    try:
        desired_entry = KvsEntry.objects.get(key=request.data['key'])
        return Response(
            {'msg': 'success', 'key': desired_entry.key, 'value': desired_entry.value, 'source': 'get_simple',
             'owner': localNode.address, 'time': desired_entry.time, 'clock': desired_entry.clock},
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
            obj, created = KvsEntry.objects.update_or_create(key=key, defaults={'value': input_value, 'time': t,
                                                                                'clock': repr(localNode.counter)})

            try:
                req.put('http://' + localNode.partition_members()[0] + '/broadcast_put/',
                        data={'key': key, 'value': input_value, 'time': t, 'clock': repr(localNode.counter)})
                print >> sys.stderr, 'broadcast success?'
            except Exception:
                print >> sys.stderr, 'broadcast put fail'
                pass

            if created:
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
                    cheq = req.get('http://' + node.address + '/get_simple/', data={'key': key})
                    if 'clock' in cheq.json():

                        # if the poll returns a vector clock sooner than our own
                        if eval((cheq.json()['clock'])[localNode.partition_id()] >
                                localNode.counter[localNode.partition_id()]):
                            # merge clocks together
                            localNode.counter = localNode.counter | eval(cheq.json()['clock'])
                            # update keys to value with more recent clock
                            KvsEntry.objects.update_or_create(key=key, defaults={'value': cheq.json()['value'],
                                                                                 'time': cheq.json()['time'],
                                                                                 'clock': cheq.json()['clock']})

                        if (eval(cheq.json()['clock'])[localNode.partition_id()] ==
                           localNode.counter[localNode.partition_id()] and not
                           KvsEntry.objects.get(key=key).key == cheq.json()['key']):

                            # tiebreak and fix if they win tiebreak
                            if str(node.address) + (cheq.json()['time']) > str(localNode.address) + str(
                                    KvsEntry.objects.get(key=key).time):
                                KvsEntry.objects.update_or_create(key=key, defaults={'value': cheq.json()['value'],
                                                                                     'time': cheq.json()['time'],
                                                                                     'clock': cheq.json()['clock']})

                except Exception:
                    print >> sys.stderr, "get poll fail"
                    continue

            try:
                desired_entry = KvsEntry.objects.get(key=key)
                return Response(
                    {'msg': 'success', 'value': desired_entry.value, 'partition_id': localNode.partition_id(),
                     'timestamp': desired_entry.time, 'causal_payload': desired_entry.clock},
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
        req.put('http://' + successor_ip + '/payload/ ', data={'load': repr(localNode.counter)})

        if method == 'GET':
            # forward request with query content
            res = req.get(url_str)

        elif method == 'PUT':
            # forward to main whether or not the request is empty
            res = req.put(url_str, data=request.data)

        return Response(res.json(), status=res.status_code)
