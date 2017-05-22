import threading

from nefertari.utils.data import *
from nefertari.utils.dictset import *
from nefertari.utils.utils import *

_split = split_strip


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]



class ThreadLocalSingleton(threading.local, metaclass=SingletonMeta):
    pass
