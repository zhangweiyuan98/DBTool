import base64


def decode(base64str):
    tmp = base64.b64decode(base64str)
    return bytearray([(b << 1 & 255) | (b >> 7) for b in tmp]).decode("utf8")


passwd = decode('qTaZKByhGbOZtKiwM6mgmg==')
passwd2 = decode('oaMZJyc0OiGwoxi5ICU1tDc=')
print(passwd)


def encode_password(password):
    password_bytes = password.encode('utf-8')
    suffix_bytes = "aqa&".encode('utf-8')
    combined_bytes = password_bytes + suffix_bytes
    encoded_bytes = base64.b64encode(combined_bytes)
    return encoded_bytes.decode('utf-8')


print(encode_password("Jjinn@Greencloud2020"))


def decode_password(password):
    encoded_bytes = password.encode('utf-8')
    password_bytes = base64.b64decode(encoded_bytes)
    decoded_str = password_bytes.decode('utf-8')
    return decoded_str[:-4]

print(decode_password("ZGV2aXNrYWlmYWFxYSY="))
