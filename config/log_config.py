import os
from typing import Dict, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowException

FAB_LOG_LEVEL: str = conf.get('logging', 'FAB_LOGGING_LEVEL').upper()

LOG_LEVEL = conf.get('logging', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('logging', 'log_format')

# COLORED_LOG_FORMAT: str = conf.get('logging', 'COLORED_LOG_FORMAT')
# COLORED_LOG: bool = conf.getboolean('logging', 'COLORED_CONSOLE_LOG')
# COLORED_FORMATTER_CLASS: str = conf.get('logging', 'COLORED_FORMATTER_CLASS')

BASE_LOG_FOLDER = conf.get('logging', 'BASE_LOG_FOLDER')
PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'child_process_log_directory')

FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow': {'format': LOG_FORMAT},
        # 'airflow_coloured': {
        #     'format': COLORED_LOG_FORMAT if COLORED_LOG else LOG_FORMAT,
        #     'class': COLORED_FORMATTER_CLASS if COLORED_LOG else 'logging.Formatter',
        # },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow',
            'stream': 'ext://sys.stdout'
        },
        'task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        'processor': {
            'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        },
    },
    'loggers': {
        'airflow.processor': {
            'handlers': ['processor'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'airflow.task': {
            'handlers': ['task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'flask_appbuilder': {
            'handler': ['console'],
            'level': FAB_LOG_LEVEL,
            'propagate': True,
        },
    },
    'root': {
        'handlers': ['console'],
        'level': LOG_LEVEL,
    },
}

##################
# Remote logging #
##################

REMOTE_LOGGING: bool = conf.getboolean('logging', 'remote_logging')

if REMOTE_LOGGING:

    ELASTICSEARCH_HOST: str = conf.get('elasticsearch', 'HOST')

    # Storage bucket URL for remote logging
    if ELASTICSEARCH_HOST:
        ELASTICSEARCH_LOG_ID_TEMPLATE: str = conf.get('elasticsearch', 'LOG_ID_TEMPLATE')
        ELASTICSEARCH_END_OF_LOG_MARK: str = conf.get('elasticsearch', 'END_OF_LOG_MARK')
        ELASTICSEARCH_FRONTEND: str = conf.get('elasticsearch', 'frontend')
        ELASTICSEARCH_WRITE_STDOUT: bool = conf.getboolean('elasticsearch', 'WRITE_STDOUT')
        ELASTICSEARCH_JSON_FORMAT: bool = conf.getboolean('elasticsearch', 'JSON_FORMAT')
        ELASTICSEARCH_JSON_FIELDS: str = conf.get('elasticsearch', 'JSON_FIELDS')

        ELASTIC_REMOTE_HANDLERS: Dict[str, Dict[str, Union[str, bool]]] = {
            'task': {
                'class': 'config.es_task_handler.ESTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                'log_id_template': ELASTICSEARCH_LOG_ID_TEMPLATE,
                'filename_template': FILENAME_TEMPLATE,
                'end_of_log_mark': ELASTICSEARCH_END_OF_LOG_MARK,
                'host': ELASTICSEARCH_HOST,
                'frontend': ELASTICSEARCH_FRONTEND,
                'write_stdout': ELASTICSEARCH_WRITE_STDOUT,
                'json_format': ELASTICSEARCH_JSON_FORMAT,
                'json_fields': ELASTICSEARCH_JSON_FIELDS,
            },
        }

        LOGGING_CONFIG['handlers'].update(ELASTIC_REMOTE_HANDLERS)
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
            "'remote_base_log_folder' option in the 'logging' section."
        )
