#! /usr/bin/env python
import logging
import argparse
import shared_utils
import json
import urlparse

logger = logging.getLogger('create_kibana_dashboards')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('/var/log/{}.log'.format(__name__))
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(file_handler)

_installers = {'fuel', 'apex', 'compass', 'joid'}

# {
#     "metrics":
#         [
#             {
#                 "type": type,           # default sum
#                 "params": {
#                     "field": field      # mandatory, no default
#                 },
#                 {metric2}
#         ],
#     "segments":
#         [
#             {
#                 "type": type,           # default date_histogram
#                 "params": {
#                     "field": field      # default creation_date
#                 },
#                 {segment2}
#         ],
#     "type": type                        # default area
# }

# see class VisualizationState for details on format
_testcases = [
    ('functest', 'Tempest',
     [
         {
             "metrics": [
                 {
                     "type": "avg",
                     "params": {
                         "field": "details.duration"
                     }
                 }
             ],
             "type": "area",
         },

         {
             "metrics": [
                 {
                     "type": "sum",
                     "params": {
                         "field": "details.tests"
                     }
                 },
                 {
                     "type": "sum",
                     "params": {
                         "field": "details.failures"
                     }
                 }
             ],
             "type": "histogram",
         },

         # add for success rate
         # {
         #     "metrics": [
         #         {
         #             "type": "avg",
         #             "params": {
         #                 "field": "details.duration"
         #             }
         #         }
         #     ]
         # }
     ]
     ),

    ('functest', 'Rally',
     []
     ),

    ('functest', 'vPing',
     []
     ),

    ('functest', 'vPing_userdata',
     []
     ),

    ('functest', 'ODL',
     []
     ),

    ('functest', 'ONOS',
     []
     ),

    ('functest', 'vIMS',
     []
     ),

    ('promise', 'promise',
     []
     ),

    ('doctor', 'doctor-notification',
     []
     )
]


class KibanaDashboard(dict):
    def __init__(self, project_name, case_name, installer, pod, versions, visualization_detail):
        super(KibanaDashboard, self).__init__()
        self.project_name = project_name
        self.case_name = case_name
        self.installer = installer
        self.pod = pod
        self.versions = versions
        self.visualization_detail = visualization_detail
        self._visualization_title = None
        self._kibana_visualizations = []
        self._kibana_dashboard = None
        self._create_visualizations()
        self._create()

    def _create_visualizations(self):
        for version in self.versions:
            self._kibana_visualizations.append(KibanaVisualization(self.project_name,
                                                                   self.case_name,
                                                                   self.installer,
                                                                   self.pod,
                                                                   version,
                                                                   self.visualization_detail))

        self._visualization_title = self._kibana_visualizations[0].vis_state_title

    def _publish_visualizations(self):
        for visualization in self._kibana_visualizations:
            url = urlparse.urljoin(base_elastic_url, '/.kibana/visualization/{}'.format(visualization.id))
            logger.debug("publishing visualization '{}'".format(url))
            shared_utils.publish_json(visualization, url)

    def _construct_panels(self):
        size_x = 6
        size_y = 3
        max_columns = 7
        column = 1
        row = 1
        panel_index = 1
        panels_json = []
        for visualization in self._kibana_visualizations:
            panels_json.append({
                "id": visualization.id,
                "type": 'visualization',
                "panelIndex": panel_index,
                "size_x": size_x,
                "size_y": size_y,
                "col": column,
                "row": row
            })
            panel_index += 1
            column += size_x
            if column > max_columns:
                column = 1
                row += size_y
        return json.dumps(panels_json, separators=(',', ':'))

    def _create(self):
        self['title'] = '{} {} {} {}'.format(self.project_name,
                                             self.case_name,
                                             self._visualization_title,
                                             self.pod)
        self.id = self['title'].replace(' ', '-')

        self['hits'] = 0
        self['description'] = "Kibana dashboard for project_name '{}', case_name '{}', installer '{}', data '{}' and" \
                              " pod '{}'".format(self.project_name,
                                                 self.case_name,
                                                 self.installer,
                                                 self._visualization_title,
                                                 self.pod)
        self['panelsJSON'] = self._construct_panels()
        self['optionsJSON'] = json.dumps({
            "darkTheme": False
        },
            separators=(',', ':'))
        self['uiStateJSON'] = "{}"
        self['version'] = 1
        self['timeRestore'] = False
        self['kibanaSavedObjectMeta'] = {
            'searchSourceJSON': json.dumps({
                "filter": [
                    {
                        "query": {
                            "query_string": {
                                "query": "*",
                                "analyze_wildcard": True
                            }
                        }
                    }
                ]
            },
                separators=(',', ':'))
        }

    def _publish(self):
        url = urlparse.urljoin(base_elastic_url, '/.kibana/dashboard/{}'.format(self.id))
        logger.debug("publishing dashboard '{}'".format(url))
        shared_utils.publish_json(self, url)

    def publish(self):
        self._publish_visualizations()
        self._publish()


class KibanaSearchSourceJSON(dict):
    """
    "filter": [
                    {"match": {"installer": {"query": installer, "type": "phrase"}}},
                    {"match": {"project_name": {"query": project_name, "type": "phrase"}}},
                    {"match": {"case_name": {"query": case_name, "type": "phrase"}}}
                ]
    """

    def __init__(self, project_name, case_name, installer, pod, version):
        super(KibanaSearchSourceJSON, self).__init__()
        self["filter"] = [
            {"match": {"project_name": {"query": project_name, "type": "phrase"}}},
            {"match": {"case_name": {"query": case_name, "type": "phrase"}}},
            {"match": {"installer": {"query": installer, "type": "phrase"}}},
            {"match": {"version": {"query": version, "type": "phrase"}}}
        ]
        if pod != 'all':
            self["filter"].append({"match": {"pod": {"query": pod, "type": "phrase"}}})


class VisualizationState(dict):
    def __init__(self, input_dict):
        """
        dict structure:
            {
            "metrics":
                [
                    {
                        "type": type,           # default sum
                        "params": {
                            "field": field      # mandatory, no default
                    },
                    {metric2}
                ],
            "segments":
                [
                    {
                        "type": type,           # default date_histogram
                        "params": {
                            "field": field      # default creation_date
                    },
                    {segment2}
                ],
            "type": type                        # default area
            }

        default modes:
            type histogram: grouped
            type area: stacked

        :param input_dict:
        :return:
        """
        super(VisualizationState, self).__init__()
        metrics = input_dict['metrics']
        segments = [] if 'segments' not in input_dict else input_dict['segments']

        graph_type = 'area' if 'type' not in input_dict else input_dict['type']
        self['type'] = graph_type

        if 'mode' not in input_dict:
            if graph_type == 'histogram':
                mode = 'grouped'
            else:
                # default
                mode = 'stacked'
        else:
            mode = input_dict['mode']
        self['params'] = {
            "shareYAxis": True,
            "addTooltip": True,
            "addLegend": True,
            "smoothLines": False,
            "scale": "linear",
            "interpolate": "linear",
            "mode": mode,
            "times": [],
            "addTimeMarker": False,
            "defaultYExtents": False,
            "setYExtents": False,
            "yAxis": {}
        }

        self['aggs'] = []

        i = 1
        for metric in metrics:
            self['aggs'].append({
                "id": str(i),
                "type": 'sum' if 'type' not in metric else metric['type'],
                "schema": "metric",
                "params": {
                    "field": metric['params']['field']
                }
            })
            i += 1

        if len(segments) > 0:
            for segment in segments:
                self['aggs'].append({
                    "id": str(i),
                    "type": 'date_histogram' if 'type' not in segment else segment['type'],
                    "schema": "metric",
                    "params": {
                        "field": "creation_date" if ('params' not in segment or 'field' not in segment['params'])
                        else segment['params']['field'],
                        "interval": "auto",
                        "customInterval": "2h",
                        "min_doc_count": 1,
                        "extended_bounds": {}
                    }
                })
                i += 1
        else:
            self['aggs'].append({
                "id": str(i),
                "type": 'date_histogram',
                "schema": "segment",
                "params": {
                    "field": "creation_date",
                    "interval": "auto",
                    "customInterval": "2h",
                    "min_doc_count": 1,
                    "extended_bounds": {}
                }
            })

        self['listeners'] = {}
        self['title'] = ' '.join(['{} {}'.format(x['type'], x['params']['field']) for x in self['aggs']
                                  if x['schema'] == 'metric'])


class KibanaVisualization(dict):
    def __init__(self, project_name, case_name, installer, pod, version, detail):
        """
        We need two things
        1. filter created from
            project_name
            case_name
            installer
            pod
            version
        2. visualization state
            field for y axis (metric) with type (avg, sum, etc.)
            field for x axis (segment) with type (date_histogram)

        :return:
        """
        super(KibanaVisualization, self).__init__()
        vis_state = VisualizationState(detail)
        self.vis_state_title = vis_state['title']
        self['title'] = '{} {} {} {} {} {}'.format(project_name,
                                                   case_name,
                                                   self.vis_state_title,
                                                   installer,
                                                   pod,
                                                   version)
        self.id = self['title'].replace(' ', '-')
        self['visState'] = json.dumps(vis_state, separators=(',', ':'))
        self['uiStateJSON'] = "{}"
        self['description'] = "Kibana visualization for project_name '{}', case_name '{}', data '{}', installer '{}'," \
                              " pod '{}' and version '{}'".format(project_name,
                                                                  case_name,
                                                                  self.vis_state_title,
                                                                  installer,
                                                                  pod,
                                                                  version)
        self['version'] = 1
        self['kibanaSavedObjectMeta'] = {"searchSourceJSON": json.dumps(KibanaSearchSourceJSON(project_name,
                                                                                               case_name,
                                                                                               installer,
                                                                                               pod,
                                                                                               version),
                                                                        separators=(',', ':'))}


def _get_pods_and_versions(project_name, case_name, installer):
    query_json = json.JSONEncoder().encode({
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}}
                ],
                "filter": [
                    {"match": {"installer": {"query": installer, "type": "phrase"}}},
                    {"match": {"project_name": {"query": project_name, "type": "phrase"}}},
                    {"match": {"case_name": {"query": case_name, "type": "phrase"}}}
                ]
            }
        }
    })

    elastic_data = shared_utils.get_elastic_data(urlparse.urljoin(base_elastic_url, '/test_results/mongo2elastic'),
                                                 query_json)

    pods_and_versions = {}

    for data in elastic_data:
        pod = data['pod_name']
        if pod in pods_and_versions:
            pods_and_versions[pod].add(data['version'])
        else:
            pods_and_versions[pod] = {data['version']}

        if 'all' in pods_and_versions:
            pods_and_versions['all'].add(data['version'])
        else:
            pods_and_versions['all'] = {data['version']}

    return pods_and_versions


def construct_dashboards():
    """
    iterate over testcase and installer
    1. get available pods for each testcase/installer pair
    2. get available version for each testcase/installer/pod tuple
    3. construct KibanaInput and append

    :return: list of KibanaDashboards
    """
    kibana_dashboards = []
    for project_name, case_name, visualization_details in _testcases:
        for installer in _installers:
            pods_and_versions = _get_pods_and_versions(project_name, case_name, installer)
            for visualization_detail in visualization_details:
                for pod, versions in pods_and_versions.iteritems():
                    kibana_dashboards.append(KibanaDashboard(project_name, case_name, installer, pod, versions,
                                                             visualization_detail))
    return kibana_dashboards


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Kibana dashboards from data in elasticsearch')
    parser.add_argument('-e', '--elasticsearch-url', default='http://localhost:9200',
                        help='the url of elasticsearch, defaults to http://localhost:9200')

    args = parser.parse_args()
    base_elastic_url = args.elasticsearch_url

    dashboards = construct_dashboards()

    for kibana_dashboard in dashboards:
        kibana_dashboard.publish()
