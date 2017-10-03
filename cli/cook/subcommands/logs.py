import json

from cook import colors
from cook.util import strip_all, print_info


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    print(colors.failed('No matching data found in %s.' % clusters_text))
    print_info('Do you need to add another cluster to your configuration?')


def show_job_logs(cluster_name, jobs):
    """TODO(DPO)"""
    pass


def show_instance_logs(cluster_name, instances):
    """TODO(DPO)"""
    pass


def show_group_logs(cluster_name, groups):
    """TODO(DPO)"""
    pass


def logs(clusters, args):
    """TODO(DPO)"""
    as_json = args.get('json')
    uuids = strip_all(args.get('uuid'))
    query_result = {}
    if as_json:
        print(json.dumps(query_result))
    else:
        for cluster_name, entities in query_result['clusters'].items():
            jobs = entities['jobs']
            instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
            groups = entities['groups']
            show_job_logs(cluster_name, jobs)
            show_instance_logs(cluster_name, instances)
            show_group_logs(cluster_name, groups)
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
