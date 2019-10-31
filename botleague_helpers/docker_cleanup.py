import datetime

import dateutil
import psutil
import docker
from loguru import logger as log
from botleague_helpers.utils import gce_instance_id


def prune():
    """
    Prune unused images (i.e. dangling images with no tag) and all containers.
    """
    if not gce_instance_id():
        log.warning('Not cleaning up docker on non-gce machines to prevent '
                    'deleting containers in dev')
        return
    dkr = docker.from_env()
    dkr.api.prune_containers()
    dkr.api.prune_images()
    check_disk_usage()


def check_disk_usage():
    disk_usage = psutil.disk_usage('/')
    if disk_usage.percent > 90 or disk_usage.free < 50e9:
        instance_id = gce_instance_id() or 'unknown'
        log.error(f'Low disk space {disk_usage} on instance: {instance_id}')


if __name__ == '__main__':
    check_disk_usage()
