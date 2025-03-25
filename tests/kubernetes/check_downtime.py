import concurrent.futures
import subprocess
import time
from distutils.spawn import find_executable
from threading import Event

import requests

import featureform

RELEASE_NAME = 'featureform'
CHART_PATH = '../../charts/featureform'
DEPLOYMENT_NAME = 'featureform-feature-server'
NAMESPACE = 'upgrade-test'

HEALTH_CHECK_URL = 'http://localhost:80'
HEALTH_CHECK_INTERVAL = 0.1  # In seconds

stop_event = Event()

client = featureform.Client(host='localhost', cert_path='../../ca.crt')

def health_check_loop(health_fn):
    number_successful_checks = 0
    while True:
        # This is set in the main thread after the rolling update is done.
        if stop_event.is_set():
            break

        try:
            health_fn()
            number_successful_checks += 1
        except Exception as e:
            raise RuntimeError('Health check failed') from e

        time.sleep(HEALTH_CHECK_INTERVAL)

    print(f'Sent {number_successful_checks} successful health checks')

def feature_serving_check():
    def check():
        client.features(
            [("avg_transactions", "quickstart")],
            {"user": "C1214240"}
        )

    return health_check_loop(check)

def http_health_check():
    def check():
        response = requests.get(HEALTH_CHECK_URL)
        if response.status_code != 200:
            print(f'Got a {response.status_code} status code when reaching health check endpoint')
            raise RuntimeError

    return health_check_loop(check)

def restart_deployment():
    print(f'Upgrading {RELEASE_NAME} with chart files at {CHART_PATH}')

    try:
        subprocess.run(
            ['kubectl', 'rollout', 'restart', 'deployment', 'featureform-feature-server', '-n', NAMESPACE],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        print('Error updating chart')
        print('Stderr of command:')
        print('---')
        print(e.stderr)
        print('---')
        raise RuntimeError('Error updating chart') from e


def get_all_deployments():
    print(f'Getting all deployments for {DEPLOYMENT_NAME}')

    try:
        subprocess.run(
            [
                'kubectl',
                'get',
                'deployments',
                '-l',
                f'app.kubernetes.io/instance={RELEASE_NAME}',
                '-n',
                NAMESPACE,
                '-o',
                'json'
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

    except subprocess.CalledProcessError as e:
        print(f'Error getting deployments: {e}')
        print('Stderr of command:')
        print('---')
        print(e.stderr)
        print('---')
        raise RuntimeError('Error getting deployments') from e


def wait_for_rollout():
    print(f'Waiting for rollout of {DEPLOYMENT_NAME} to complete')

    try:
        subprocess.run(
            ['kubectl', 'rollout', 'status', f'deployment/{DEPLOYMENT_NAME}', '-n', NAMESPACE],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print('Rollout completed successfully')
    except subprocess.CalledProcessError as e:
        print('Error waiting for rollout: {}')
        print('Stderr of command:')
        print('---')
        print(e.stderr)
        print('---')
        raise RuntimeError('Rollout failed') from e


def check_dependencies():
    executables = ['kubectl', 'helm', 'featureform']

    not_found = list(
        filter(
            lambda exe: find_executable(exe) is None,
            executables
        )
    )

    if len(not_found) > 0:
        raise RuntimeError(f'Could not find executables {not_found} in PATH')


def main():
    print('Checking dependencies are installed')
    check_dependencies()
    print('All dependencies are installed')

    print('Starting rolling update')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(feature_serving_check)
        print('Health check is running')
        try:
            restart_deployment()

            # Wait for a few more health checks to run
            time.sleep(30)
            stop_event.set()

            future.result()
        except Exception as e:
            stop_event.set()
            print('❌  Test failure, error during rolling update:', e)
            raise e

    print('✅  Test successful, no downtime detected during rolling update')


if __name__ == '__main__':
    main()
