import logging
import sys

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'databricks': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        }
    },
    'handlers': {
        'databricks_console': {
            'class': 'logging.StreamHandler',
            'formatter': 'databricks',
            'level': 'INFO',
            'stream': sys.stdout,  # Databricks console outputs from stdout
        }
    },
    'loggers': {
        '': {
            'handlers': ['databricks_console'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
}

def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
