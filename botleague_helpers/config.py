import inspect
import os

import github


class Config:
    # Constants
    should_use_firestore = os.environ.get(
        'SHOULD_USE_FIRESTORE', 'true') == 'true'
    is_test = 'IS_TEST' in os.environ
    should_gen_key = 'should_gen_leaderboard'
    token_name = 'LEADERBOARD_GITHUB_TOKEN'

    # Properties that will change during tests
    # --------------------------------------------------------------------------
    _github_token: str = None
    _firebase_initialized: bool = False

    @property
    def github_token(self) -> str:
        test_name = get_test_name_from_callstack()
        if test_name:
            print('In test %s so returning "" for github token' % test_name)
            ret = ''
        elif self.is_test:
            print('IS_TEST is set, so returning "" for github token')
            ret = ''
        elif self.token_name in os.environ:
            print('Found %s in environment' % self.token_name)
            ret = os.environ[self.token_name]
        elif not self.should_use_firestore:
            print('SHOULD_USE_FIRESTORE is false, so returning "" '
                  'for github token')
            ret = ''
        elif self._github_token is None:
            self._ensure_firebase_initialized()
            from firebase_admin import firestore
            print('Obtaining secrets from Firestore...')
            secrets = firestore.client().collection('secrets')
            ret = secrets.document(self.token_name).get().\
                to_dict()['token']
        else:
            ret = self._github_token
        self._github_token = ret
        return ret

    def _ensure_firebase_initialized(self):
        if not self._firebase_initialized:
            import firebase_admin

            try:
                firebase_admin.initialize_app()
            except Exception as e:
                raise RuntimeError(
                    'Could not initialize firestore, '
                    'set SHOULD_USE_FIRESTORE=false'
                    ' locally to use temp storage.')
            self._firebase_initialized = True


def get_test_name_from_callstack() -> str:
    ret = ''
    for level in inspect.stack():
        fn = level.function
        test_prefix = 'test_'
        if fn.startswith(test_prefix):
            test_name = fn[len(test_prefix):]
            ret = test_name
    return ret


if 'GITHUB_DEBUG' in os.environ:
    github.enable_console_debug_logging()


c = Config()
