import base64

def encode_password(password):
    """加密"""
    password_bytes = password.encode('utf-8')
    suffix_bytes = "aqa&".encode('utf-8')
    combined_bytes = password_bytes + suffix_bytes
    encoded_bytes = base64.b64encode(combined_bytes)
    return encoded_bytes.decode('utf-8')


def decode_password(password):
    """解密"""
    encoded_bytes = password.encode('utf-8')
    password_bytes = base64.b64decode(encoded_bytes)
    decoded_str = password_bytes.decode('utf-8')
    return decoded_str[:-4]
