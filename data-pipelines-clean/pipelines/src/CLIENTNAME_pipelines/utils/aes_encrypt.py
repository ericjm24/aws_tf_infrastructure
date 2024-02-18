from Crypto.Cipher import AES
from argparse import ArgumentParser
from string import ascii_letters, digits
from random import choices


def decrypt_file(file, key):
    nonce, tag, ciphertext = [file.read(x) for x in (16, 16, -1)]
    cipher = AES.new(key, AES.MODE_EAX, nonce)
    data = cipher.decrypt_and_verify(ciphertext, tag)
    return data


def encrypt_file(file_name, key):
    cipher = AES.new(key, AES.MODE_EAX)
    with open(file_name, mode="rb") as file:
        ciphertext, tag = cipher.encrypt_and_digest(file.read())
    with open(file_name + ".bin", "wb") as file_out:
        [file_out.write(x) for x in (cipher.nonce, tag, ciphertext)]


if __name__ == "__main__":
    arg_parse = ArgumentParser()
    arg_parse.add_argument("path", type=str, help="Path of file to be encrypted.")
    arg_parse.add_argument("--key", type=str)

    vargs = arg_parse.parse_args()
    key = vargs.key or "".join(choices(ascii_letters + digits, k=16))
    encrypt_file(vargs.path, key.encode("utf-8"))
    print(f"Key: {key}")
