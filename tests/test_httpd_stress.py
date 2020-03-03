from concurrent.futures import ThreadPoolExecutor
from random import choice, random
from urllib.request import urlopen, Request
from time import sleep
import json
from http import HTTPStatus

from test_httpdb import docker_fixture
import pytest


@pytest.fixture
def server():
    create, cleanup = docker_fixture()
    yield create()
    cleanup()


projects = [f'prj-{i}' for i in range(3)]
uids = [f'uid-{i}' for i in range(5)]

headers = {
    'Content-Type': 'application/json',
}

run1 = {
    'metadata': {
        'name': 'train',
        'uid': 'c613319190f54209b867986753fd0c6d',
        'iteration': 0,
        'project': 'image-classification',
        'labels': {
            'workflow': 'bce78720-09bd-400f-a5cf-989a2bfa172a',
            'kind': 'mpijob',
            'owner': 'root',
            'mlrun/job': 'train-a4696566',
            'host': 'train-a4696566-worker-1'
            },
        'annotations': {},
        },
    'spec': {
        'function':
            (
                'image-classification/horovod-trainer:'
                '24d41dd3350632dc8920b63d1f8d2f107283a68d'
            ),
        'log_level': 'info',
        'parameters': {
            'epochs': 1,
            'checkpoints_dir': '/User/mlrun/examples/checkpoints',
            'model_path': '/User/mlrun/examples/models/cats_n_dogs.h5',
            'data_path': '/User/mlrun/examples/images/cats_n_dogs',
            'use_gpu': True,
            'image_width': 128,
            },
        'outputs': ['model'],
        'output_path':
        'v3io:///users/admin/mlrun/kfp/bce78720-09bd-400f-a5cf-989a2bfa172a/',
        'inputs': {
            'categories_map':
            '/User/mlrun/examples/images/categories_map.json',
            'file_categories':
            '/User/mlrun/examples/images/file_categories_df.csv',
            },
        'data_stores': [],
    },
    'status': {
        'state': 'running',
        'results': {},
        'start_time': '2020-02-28 16:17:27.678750',
        'last_update': '2020-02-28 16:17:27.678756',
        'artifacts': []
    },
}


run2 = {
    'metadata': {
        'name': 'train',
        'uid': 'c613319190f54209b867986753fd0c6d',
        'iteration': 0,
        'project': 'image-classification',
        'labels': {
            'workflow': 'bce78720-09bd-400f-a5cf-989a2bfa172a',
            'kind': 'mpijob',
            'owner': 'root',
            'mlrun/job': 'train-a4696566',
            'host': 'train-a4696566-worker-2',
        },
        'annotations': {},
    },
    'spec': {
        'function':
            (
                'image-classification/horovod-trainer:'
                '24d41dd3350632dc8920b63d1f8d2f107283a68d'
            ),
        'log_level': 'info',
        'parameters': {
            'epochs': 1,
            'checkpoints_dir': '/User/mlrun/examples/checkpoints',
            'model_path': '/User/mlrun/examples/models/cats_n_dogs.h5',
            'data_path': '/User/mlrun/examples/images/cats_n_dogs',
            'use_gpu': True,
            },
        'outputs': ['model'],
        'output_path':
        'v3io:///users/admin/mlrun/kfp/bce78720-09bd-400f-a5cf-989a2bfa172a/',
        'inputs': {
            'categories_map': '/User/mlrun/examples/images/cmap.json',
            'file_categories': '/User/mlrun/examples/images/fcat_df.csv',
            },
        'data_stores': [],
    },
    'status': {
        'state': 'running',
        'results': {},
        'start_time': '2020-02-28 16:17:27.705642',
        'last_update': '2020-02-28 16:17:27.705646',
        'artifacts': [],
    },
}


def worker(url):
    sleep(random() / 10.)
    prj, uid = choice(projects), choice(uids)
    url = f'{url}/api/run/{prj}/{uid}'
    obj = run1 if random() > 0.5 else run2
    data = json.dumps(obj).encode()

    req = Request(url, data, headers=headers)
    resp = urlopen(req)
    if resp.status != HTTPStatus.OK:
        raise AssertionError(f'{url} -> {resp.status}')


def test_stress(server):
    pool = ThreadPoolExecutor()
    with pool:
        for i in range(1237):
            pool.submit(worker, server.url)
