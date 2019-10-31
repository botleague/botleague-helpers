import json
import os
import os.path as p
import sys
from subprocess import PIPE, Popen
from typing import Union

import requests
from botleague_helpers.config import blconfig
from botleague_helpers.db import get_db
from box import Box, BoxList

from loguru import logger as log

from github import UnknownObjectException


def get_file_from_github(repo, filename, ref=None):
    """@:param filename: relative path to file in repo"""
    try:
        args = [filename]
        if ref is not None:
            args.append(ref)
        contents = repo.get_contents(*args)
        content_str = contents.decoded_content.decode('utf-8')
    except UnknownObjectException:
        log.error('Unable to find %s in %s', filename, repo.html_url)
        content_str = ''
    ret = get_str_or_box(content_str, filename)
    return ret


def get_str_or_box(content_str, filename):
    if filename.endswith('.json') and content_str:
        ret = Box(json.loads(content_str))
    else:
        ret = content_str
    return ret


def read_box(json_filename) -> Box:
    ret = Box().from_json(filename=json_filename)
    return ret

def write_json(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2)


def read_json(filename):
    with open(filename) as file:
        results = json.load(file)
    return results


def write_file(content, path):
    with open(path, 'w') as f:
        f.write(content)


def read_file(path):
    with open(path) as f:
        ret = f.read()
    return ret


def read_lines(path):
    content = read_file(path)
    lines = content.split()
    return lines


def append_file(path, strings):
    with open(path, 'a') as f:
        f.write('\n'.join(strings) + '\n')


def exists_and_unempty(problem_filename):
    return p.exists(problem_filename) and os.stat(problem_filename).st_size != 0


def is_docker():
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )


def generate_rand_alphanumeric(num_chars):
    from secrets import choice
    import string
    alphabet = string.ascii_uppercase + string.digits
    ret = ''.join(choice(alphabet) for _ in range(num_chars))
    return ret


def trigger_leaderboard_generation():
    db = get_db(collection_name=blconfig.botleague_collection_name)
    db.set(blconfig.should_gen_key, True)


def get_liaison_db_store():
    ret = get_db(collection_name='botleague_liaison')
    return ret


def dbox(obj=None, **kwargs):
    if kwargs:
        obj = dict(kwargs)
    else:
        obj = obj or {}
    return Box(obj, default_box=True)


def is_json(string: str):
    try:
        json.loads(string)
    except ValueError:
        return False
    return True


def box2json(box: Union[Box, BoxList]):
    return box.to_json(indent=2, default=str, sort_keys=True)


def find_replace(search_dict, field_value, replace=None):
    """
    Takes a dict with nested lists and dicts,
    and searches all dicts for a value of the field
    provided, replacing if desired.
    """
    fields_found = []

    for key, value in search_dict.items():

        if value == field_value:
            fields_found.append(value)
            if replace:
                search_dict[key] = replace

        elif isinstance(value, dict):
            results = find_replace(value, field_value, replace)
            for result in results:
                fields_found.append(result)

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    more_results = find_replace(item, field_value, replace)
                    for another_result in more_results:
                        fields_found.append(another_result)

    return fields_found


def get_upload_to_jist_fn():
    # TODO: Move these to constants, but make gist uploading deepdrive agnostic
    AWS_BUCKET = 'deepdrive'
    AWS_BUCKET_URL = 'https://s3-us-west-1.amazonaws.com/' + AWS_BUCKET
    YOU_GET_MY_JIST_URL = AWS_BUCKET_URL + '/yougetmyjist.json'
    closure = Box()
    closure.you_get_my_jist = None

    def do_it(name: str, content: str, public: bool):
        files = [f'/tmp/slack-message-tmp-{generate_rand_alphanumeric(9)}']
        with open(files[0], 'w') as tmp_file:
            tmp_file.write(content)

        gist_env = os.environ.copy()
        if not closure.you_get_my_jist:
            # Lazy load
            closure.you_get_my_jist = \
                requests.get(YOU_GET_MY_JIST_URL).text.strip()

        gist_env['YOU_GET_MY_JIST'] = closure.you_get_my_jist
        if os.path.dirname(sys.executable) not in os.environ['PATH']:
            gist_env['PATH'] = os.path.dirname(sys.executable) + ':' + gist_env['PATH']
        opts = '--public' if public else ''
        cmd = 'gist {opts} create {gist_name} {files}'
        filelist = ' '.join('"%s"' % f for f in files)
        cmd = cmd.format(gist_name=name, files=filelist, opts=opts)
        output, ret_code = run_command(cmd, env=gist_env, verbose=True)
        if ret_code != 0:
            log.warn('Could not upload gist. \n%s' % (output,))
        url = output if ret_code == 0 else None
        os.remove(files[0])
        return url
    return do_it

upload_to_gist = get_upload_to_jist_fn()


def run_command(cmd, cwd=None, env=None, throw=True, verbose=False, print_errors=True):
    def say(*args):
        if verbose:
            print(*args)
    say(cmd)
    if not isinstance(cmd, list):
        cmd = cmd.split()
    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, cwd=cwd, env=env)
    result, err = process.communicate()
    if not isinstance(result, str):
        result = ''.join(map(chr, result))
    result = result.strip()
    say(result)
    if process.returncode != 0:
        if not isinstance(err, str):
            err = ''.join(map(chr, err))
        err_msg = ' '.join(cmd) + ' finished with error ' + err.strip()
        if throw:
            raise RuntimeError(err_msg)
        elif print_errors:
            print(err_msg)
    return result, process.returncode


def gce_instance_id():
   meta_url = 'http://metadata.google.internal/computeMetadata/v1/instance'
   id_url = f'{meta_url}/id'
   try:
       resp = requests.get(id_url, headers={'Metadata-Flavor': 'Google'})
       if resp.ok:
           return resp.text
   except requests.ConnectionError:
       pass
   return None


def ensure_nvidia_docker_runtime():
    import docker
    dkr = docker.from_env()
    if 'nvidia' not in dkr.api.info()['Runtimes']:
        # TODO service docker restart
        pass


if __name__ == '__main__':
    ensure_nvidia_docker_runtime()


