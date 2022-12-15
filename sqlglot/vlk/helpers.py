import re

def norm_id(name):
    new_name = re.sub('[ -]+', '_', name).lower()
    new_name = re.sub('[^0-9a-zA-Z_]+', '', new_name)
    return new_name