import os

from leaderboard_generator import logs

log = logs.get_log(__name__)

# TODO: Move this and key_value_store into shared botleague-gcp pypi package

# For local testing, set SHOULD_USE_FIRESTORE=false in your environment
from google.auth.exceptions import DefaultCredentialsError

SHOULD_USE_FIRESTORE = os.environ.get('SHOULD_USE_FIRESTORE', 'true') == 'true'
SHOULD_GEN_KEY = 'should_gen_leaderboard'

TOKEN_NAME = 'LEADERBOARD_GITHUB_TOKEN'
if SHOULD_USE_FIRESTORE:
    log.info('Connecting to Firestore')
    import firebase_admin
    from firebase_admin import firestore

    try:
        firebase_admin.initialize_app()
    except Exception as e:
        raise RuntimeError(
            'Could not initialize firestore, set SHOULD_USE_FIRESTORE=false'
            ' locally to use temp storage.')
    log.info('Obtaining secrets from Firestore...')
    SECRETS = firestore.client().collection('secrets')
    if TOKEN_NAME in os.environ:
        GITHUB_TOKEN = os.environ[TOKEN_NAME]
    else:
        GITHUB_TOKEN = SECRETS.document(TOKEN_NAME).get().to_dict()['token']
    log.info('Secrets loaded')
else:
    # We want botleague_gcp to be extractable as a standalone module,
    # so don't import leaderboard_generator.config
    if 'IS_TEST' not in os.environ:
        # For local testing against GitHub
        if TOKEN_NAME not in os.environ:
            raise RuntimeError('%s not in env' % TOKEN_NAME)
        GITHUB_TOKEN = os.environ[TOKEN_NAME]
