import logging
import logging.config
import sys
import colorlog

# Create a color formatter using colorlog
formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

# Create a stream handler using the color formatter
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'databricks_console': {
            'class': 'logging.StreamHandler',
            'formatter': 'color',
            'level': 'INFO',
            'stream': sys.stdout,
        }
    },
    'loggers': {
        # Suppress other libraries and root logger
        '': {
            'handlers': ['databricks_console'],
            'level': 'WARNING',
            'propagate': False
        },
        # Custom logger for your app
        'my_app': {
            'handlers': ['databricks_console'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

def setup_logging():
    # Manually configure the handler with colorlog
    logging.config.dictConfig(LOGGING_CONFIG)
    logging.getLogger('databricks_console').handlers = [console_handler]
