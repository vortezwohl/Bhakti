NULL = 0
UTF_8 = 'utf-8'
DEFAULT_EOF_STR = '<eof>'
DEFAULT_EOF = DEFAULT_EOF_STR.encode(UTF_8)
DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 23860
DEFAULT_TIMEOUT = 4.0
DEFAULT_BUFFER_SIZE = 256


def EMPTY_STR():
    return str('')


def EMPTY_BLANK():
    return str(' ')


def EMPTY_LIST():
    return list([])


def EMPTY_SET():
    return set({})


def EMPTY_DICT():
    return dict({})


def EMPTY_TUPLE():
    return tuple(())
