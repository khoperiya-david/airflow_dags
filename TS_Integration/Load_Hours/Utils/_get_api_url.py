import sys
# Setting up logging
import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def get_api_url(type: str):
    try:
        log.info('[Custom] Getting TimeSheets 2.0 api url...')
        root_path = 'https://api.timesheets.mts.ru/api/'
        if type == 'raw':
            rel_path = 'timeallocations'
        elif type == 'normalised':
            rel_path = 'conversion/time-allocations'
        else: 
            rel_path = 'period'
        path = root_path + rel_path
    except Exception as e:
        log.error('[Custom] Getting TimeSheets 2.0 api url. Failed.')
        log.error(e)
        sys.exit(1)
    else:
        log.info('[Custom] Getting TimeSheets 2.0 api url. Success.')
        return path