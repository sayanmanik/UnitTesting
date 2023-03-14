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


def get_create_batch_entry():
    curr_batch_id = 100
    batch_start_ts = '2023-02-10 08:15:44.287'
    delta_start_ts = '2023-02-10'
    delta_end_ts = '2023-02-10'
    return curr_batch_id, batch_start_ts, delta_start_ts, delta_end_ts


def get_upsert_task_status():
    etl_control_task_id = 12
    task_status = 'SUCCESS'

    return etl_control_task_id, task_status


def get_close_batch_status():
    batch_id = 10
    previous_status = 'STARTED'
    batch_status = 'SUCCESS'

    return batch_id, previous_status, batch_status


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(get_settings())
#    params = {"ENV" : "nprod_dev", "APP_ACRONYM": "oma", "SOURCE_SYS": "amac"}



    # print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
