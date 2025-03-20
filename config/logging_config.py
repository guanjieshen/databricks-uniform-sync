import logging
import logging.config
import sys
import colorlog

# Define a basic logging config (without the formatter)
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'databricks_console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'stream': sys.stdout,
        }
    },
    'loggers': {
        '': {
            'handlers': ['databricks_console'],
            'level': 'WARNING',
            'propagate': False
        },
        'dbx_to_sf_mirror': {
            'handlers': ['databricks_console'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

def setup_logging():
    # First configure using dictConfig
    logging.config.dictConfig(LOGGING_CONFIG)
    
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

    # Create a stream handler using colorlog
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Attach colorlog handler to the 'dbx_to_sf_mirror' logger
    logger = logging.getLogger('dbx_to_sf_mirror')
    logger.handlers.clear()  # Remove existing handlers
    logger.addHandler(console_handler)