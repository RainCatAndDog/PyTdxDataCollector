# -*- coding: UTF-8 -*-

import os
import yaml

def load_settings():
    settings_file = os.path.join(os.path.dirname(__file__), 'settings')
    with open(settings_file, 'rb') as file:
        file_contents = file.read()
    settings = yaml.load(file_contents, Loader=yaml.FullLoader)
    return settings

settings = load_settings()
#print(settings)
