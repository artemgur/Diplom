import hashlib


# Decorates hashlib functions
# Decorated functions can take any object as input and return int
def hashlib_int_decorator(hashlib_function):

    def decorated(element_to_hash):
        encoded_input = str(element_to_hash).encode()  # TODO is this way to hash anything good?
        hash_result = hashlib_function(encoded_input).hexdigest()
        return int(hash_result, 16)

    return decorated


sha256 = hashlib_int_decorator(hashlib.sha256)

md5 = hashlib_int_decorator(hashlib.md5)
