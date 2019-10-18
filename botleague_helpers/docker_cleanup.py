import psutil
import docker
import dateutil.parser
import datetime
import requests
from loguru import logger as log
from utils import on_gce


def remove_old(weeks=4):
    """
    Remove containers and images more than @param:weeks old and prune all
    images to remove dangling images no longer referenced by stopped or
    running containers.
    :param weeks:
    :return:
    """
    if not on_gce():
        log.warning('Not cleaning up docker on non-gce machines to prevent '
                    'deleting containers in dev')
        return
    dkr = docker.from_env()
    until = f'{weeks * 24 * 7}h'
    containers = dkr.containers.list(all=True)
    for container in containers:
        finished_at_str = container.attrs['State']['FinishedAt']
        finished_at = dateutil.parser.parse(finished_at_str).\
            replace(tzinfo=None)
        cutoff = datetime.datetime.now() - datetime.timedelta(7 * weeks)
        if finished_at < cutoff:
            log.info(f'Removed container {container.attrs}')
            container.remove(v=True)
            for tag in container.image.tags:
                try:
                    dkr.api.remove_image(tag)
                except Exception as e:
                    log.warning(f'Could not remove image due to {e}')
                else:
                    log.info(f'Removed image {tag}')


    dkr.api.prune_containers(filters=dict(until=until))
    dkr.api.prune_images()
    disk_usage = psutil.disk_usage('/')
    if disk_usage.percent > 90 or disk_usage.remaining < 50e9:
        log.error(f'Low disk space {disk_usage}')




if __name__ == '__main__':
    remove_old()
