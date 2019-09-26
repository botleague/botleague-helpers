import sys

from box import Box
from loguru import logger as log

POSTFIX = '_encrypted'
DEFAULT_DB_NAME = 'secrets'

def encrypt_db_key(unencrypted_value, key, db=None):
    from botleague_helpers.db import get_db
    db = db or get_db(DEFAULT_DB_NAME)
    key = f'{key}{POSTFIX}'
    if isinstance(unencrypted_value, dict):
        encrypted_value = dict()
        for k, v in unencrypted_value.items():
            encrypted_value[k] = encrypt_symmetric(v)
        db.set(key, encrypted_value)
    else:
        db.set(key, encrypt_symmetric(unencrypted_value))


def decrypt_db_key(key, db=None):
    from botleague_helpers.db import get_db
    db = db or get_db(DEFAULT_DB_NAME)
    if not key.endswith(POSTFIX):
        key = f'{key}{POSTFIX}'
    encrypted_value = db.get(key)
    if isinstance(encrypted_value, Box):
        if 'token' in encrypted_value:
            encrypted_value = encrypted_value.token
    ret = decrypt_symmetric(encrypted_value)
    return ret


def encrypt_symmetric(plaintext, project_id='silken-impulse-217423',
                      location_id='global',
                      key_ring_id='deepdrive', crypto_key_id='deepdrive'):
    """Encrypts input plaintext data using the provided symmetric CryptoKey."""

    from google.cloud import kms_v1

    # Creates an API client for the KMS API.
    client = kms_v1.KeyManagementServiceClient()

    # The resource name of the CryptoKey.
    name = client.crypto_key_path_path(project_id, location_id, key_ring_id,
                                       crypto_key_id)

    # Use the KMS API to encrypt the data.
    response = client.encrypt(name, plaintext.encode())
    print(response.ciphertext)
    return response.ciphertext


def decrypt_symmetric(ciphertext, project_id='silken-impulse-217423',
                      location_id='global',
                      key_ring_id='deepdrive', crypto_key_id='deepdrive'):
    """Decrypts input ciphertext using the provided symmetric CryptoKey."""

    from google.cloud import kms_v1

    # Creates an API client for the KMS API.
    client = kms_v1.KeyManagementServiceClient()

    # The resource name of the CryptoKey.
    name = client.crypto_key_path_path(project_id, location_id, key_ring_id,
                                       crypto_key_id)
    # Use the KMS API to decrypt the data.
    response = client.decrypt(name, ciphertext)
    ret = response.plaintext.decode()
    return ret


def main():
    if '--decrypt' in sys.argv:
        name = sys.argv[-1]
        print(decrypt_db_key(name))
    elif '--encrypt' in sys.argv:
        name = sys.argv[-2]
        value = sys.argv[-1]
        encrypt_db_key(value, name)


# Usage
# Decrypt:
#   python crypto.py --decrypt MYSECRETNAME
# Encrypt:
#   python crypto.py --encrypt NEW_NAME MYVALUE
if __name__ == '__main__':
    main()
