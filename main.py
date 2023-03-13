# This file will be replaced by stg_to_delta.py

import json

ENV = 'nprod_dev'
APP_ACRONYM = 'oma'
SOURCE_SYS = 'amac'
env = ['nprod_dev', 'local', 'plab', 'prod']

# function for parameters


def env_param():
    return ENV


def app_acronym_param():
    return APP_ACRONYM


def source_sys_param():
    return SOURCE_SYS


def get_settings():
    file = open('delta_settings_amac_nprod_dev.json')
    data = json.load(file)
    file.close()
    return data


def get_staging_container():
    file = open('delta_settings_amac_nprod_dev.json')
    data = json.load(file)
    file.close()
    return data['StagingContainer']


def get_log_container():
    file = open('delta_settings_amac_nprod_dev.json')
    data = json.load(file)
    file.close()
    return data['LogContainer']


def get_target_container():
    file = open('delta_settings_amac_nprod_dev.json')
    data = json.load(file)
    file.close()
    return data['TargetContainer']


def get_containers(param):
    if param == 'StagingContainer':
        return get_staging_container()
    elif param == 'LogContainer':
        return get_log_container()
    elif param == 'TargetContainer':
        return get_target_container()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(get_settings())
#    params = {"ENV" : "nprod_dev", "APP_ACRONYM": "oma", "SOURCE_SYS": "amac"}



    # print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
