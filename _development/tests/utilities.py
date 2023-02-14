def print_response(r, prefix=None):
    if prefix is not None:
        print(prefix, r.status_code, r.content.decode())
    else:
        print(r.status_code, r.content.decode())