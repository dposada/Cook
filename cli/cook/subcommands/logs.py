import json

from cook import colors
from cook.subcommands.show import query
from cook.util import strip_all, print_info


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    print(colors.failed('No matching data found in %s.' % clusters_text))
    print_info('Do you need to add another cluster to your configuration?')


def retrieve_paths(instance):
    """TODO(DPO)"""
    print(instance)
    return []


def retrieve_logs(clusters, uuids, path):
    """TODO(DPO)"""
    query_result = query(clusters, uuids)
    for cluster_name, entities in query_result['clusters'].items():
        ij_pairs = [(i, j) for j in entities['jobs'] for i in j['instances']]
        ij_pairs.extend([(i, j) for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids])
        id_to_instance_job_pair = {i['task_id']: (i, j) for (i, j) in ij_pairs}
        # print(json.dumps(id_to_instance_job_pair, indent=2))
        for (instance, job) in id_to_instance_job_pair.values():
            paths = [path] if path else retrieve_paths(instance)
            for path in paths:
                header = '=== Job: %s (%s), Instance: %s, Path: %s ===' % \
                         (job['uuid'], job['name'], instance['task_id'], path)
                print(header)
    return {}


def logs(clusters, args):
    """TODO(DPO)"""
    as_json = args.get('json')
    uuids = strip_all(args.get('uuid'))
    path = args.get('path')
    query_result = retrieve_logs(clusters, uuids, path)
    if as_json:
        print(json.dumps(query_result))
    else:
        print('TODO')
    if query_result['count'] > 0:
        return 0
    else:
        if not as_json:
            print_no_data(clusters)
        return 1


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('logs', help='TODO(DPO)')
    show_parser.add_argument('uuid', nargs='+')
    show_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    show_parser.add_argument('--path', '-p', help='TODO(DPO)')
    return logs
