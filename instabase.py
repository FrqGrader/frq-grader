import json
import requests
import time

# Instabase API Endpoints
HOST = 'https://www.instabase.com'
NLP_API = '/api/v1/nlp'
DRIVE_API = '/api/v1/drives'
FLOW_API = '/api/v1/flow/run_flow_async'
JOB_STATUS_API = '/api/v1/jobs/status?job_id='

DEFAULT_PIPELINE = {
    "steps": [{
        "name": "Sentiment Analysis"
    }, {
        "name": "Entity Action Analysis"
    }],
    "requested_services": ["app", "account", "files", "username"]
}


class IBApi(object):
    def __init__(self, api_key):
        self.api_key = api_key

    def _get_header(self):
        return {'Authorization': 'Bearer {}'.format(self.api_key)}
    def compute_Weights(self):
        url = HOST + 'api/v1/cluster/compute_weights'
        api_args = {"repo_name": "GraderWorkspace", "repo_owner": "jsu", "root_dir": "fs/Instabase%20Drive/Grading/input"}
        headers = self._get_header()
        headers['Instabase-API-Args'] = json.dumps(api_args)
        r = requests.get(url, headers=headers)
        resp = json.loads(r.headers['Instabase-API-Resp'])
        
        weights = read_file("jsu/GraderWorkspace/fs/Instabase%20Drive/Grading/input/weights.json")
        return weights
        
    def read_file(self, location):
        url = HOST + DRIVE_API + u'/' + location.lstrip(u'/')
        api_args = dict(type='file', get_content=True, range='bytes=0--1')
        headers = self._get_header()
        headers['Instabase-API-Args'] = json.dumps(api_args)
        r = requests.get(url, headers=headers)

        resp = json.loads(r.headers['Instabase-API-Resp'])
        data = r.content
        return data

    def save_file(self, location, contents, mime_type):
        url = HOST + DRIVE_API + u'/' + location.lstrip(u'/')
        api_args = dict(
            type='file', cursor=0, if_exists='overwrite', mime_type=mime_type)

        headers = self._get_header()
        headers['Instabase-API-Args'] = json.dumps(api_args)

        data = contents
        resp = requests.post(url, data=data, headers=headers).json()

    def poll_until_finished(self, job_id, timeout=20):
        start = time.time()
        while True:
            url = HOST + JOB_STATUS_API + str(job_id)
            poll_res = requests.get(url, headers=self._get_header())
            res = poll_res.json()
            if res.get('state') == 'DONE':
                return res['results']
            if time.time() - start > timeout:
                print("JOB POLLING TIMED OUT")
                exit(1)
            time.sleep(0.5)

    def run_flow(self, input_dir, flow_path):
        args = {'input_folder': input_dir, 'ibflow_path': flow_path}
        url = HOST + FLOW_API
        flow_resp = requests.post(
            url, headers=self._get_header(), json=args).json()
        output_location = flow_resp['data']['output_folder']
        self.poll_until_finished(flow_resp['data']['job_id'], timeout=100000)


class NLPApi(object):
    def __init__(self, api_key):
        self.api_key = api_key
        self.output_location = None

    def _get_header(self):
        return {'Authorization': 'Bearer {}'.format(self.api_key)}
    def poll_until_finished(self, job_id, timeout=20):
        start = time.time()
        while True:
            url = HOST + JOB_STATUS_API + str(job_id)
            poll_res = requests.get(url, headers=self._get_header())
            res = poll_res.json()
            if res.get('state') == 'DONE':
                return res['results']
            if time.time() - start > timeout:
                print("JOB POLLING TIMED OUT")
                exit(1)
            time.sleep(0.5)

    def set_output_location(self, location):
        self.output_location = location

    def run_pipeline(self,
                     entity,
                     custom_search_profiles,
                     analysis_profiles,
                     entity_aliases=[],
                     output_location=None,
                     num_articles=20):

        parameters = {
            "sentiment_entity": entity,
            "sentiment_entity_aliases": entity_aliases,
            "action_entity": entity,
            "action_entity_aliases": entity_aliases,
            "custom_profiles": custom_search_profiles,
            "analysis_profiles": analysis_profiles
        }

        output_loc = output_location or self.output_location
        if not output_loc:
            raise ValueError(
                'Must provide an output location to save search results')

            # Run the pipeline
        form = dict(
            input_generation={
                'custom_search_profiles': custom_search_profiles,
                'profiles': analysis_profiles,
                'entity': entity,
                'max_results': num_articles
            },
            parameters=parameters,
            pipeline=DEFAULT_PIPELINE)

        url = HOST + NLP_API + u'/' + output_loc.strip(u'/') + u'/pipeline'
        resp = requests.post(url, json=form, headers=self._get_header()).json()
        results = self.poll_until_finished(resp['job_id'], timeout=1000)
        return results

    def run_pipeline_on_existing(self,
                                 entity,
                                 custom_search_profiles,
                                 analysis_profiles,
                                 input_dir,
                                 entity_aliases=[]):

        parameters = {
            "sentiment_entity": entity,
            "sentiment_entity_aliases": entity_aliases,
            "action_entity": entity,
            "action_entity_aliases": entity_aliases,
            "custom_profiles": custom_search_profiles,
            "analysis_profiles": analysis_profiles
        }

        # Run the pipeline
        form = dict(
            input_generation=None,
            parameters=parameters,
            pipeline=DEFAULT_PIPELINE)

        url = HOST + NLP_API + u'/' + input_dir.strip(u'/') + u'/pipeline'
        print(url)
        resp = requests.post(url, json=form, headers=self._get_header()).json()
        results = self.poll_until_finished(resp['job_id'], timeout=1000)
        return results


#Begin Script
f = open("input.txt")
content = f.readlines()
passedString = ""
for it in content:
    passedString = passedString + it.rstrip()+" "
API = IBApi("vRyWDppiZbL7Tf1OphvabTvuGfZnmk")
API.save_file("jsu/GraderWorkspace/fs/Instabase%20Drive/Grading/input/temp", passedString, 'txt')
f.close()
answerKey = API.read_file("jsu/GraderWorkspace/fs/Instabase%20Drive/Grading/input/answer")

