from loguru import logger as log


def encrypt_db_key(db, key):
    unencrypted_value = db.get(key)
    new_key = f'{key}_encrypted'
    if isinstance(unencrypted_value, dict):
        encrypted_value = dict()
        for k, v in unencrypted_value.items():
            encrypted_value[k] = encrypt_symmetric(v)
        db.set(new_key, encrypted_value)
    else:
        db.set(new_key, encrypt_symmetric(unencrypted_value))
    log.warning(f'Be sure to delete your old plaintext values at {key}')


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
