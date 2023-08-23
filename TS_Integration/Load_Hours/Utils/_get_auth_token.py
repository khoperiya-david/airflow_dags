import sys
# Setting up logging
import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


def get_auth_token(params: dict) -> str:
    try:
        import requests
        from datetime import datetime, timedelta
        log.info('[Custom] Getting auth token...')
        auth_url: str = _get_auth_credentials(
            params['TS_environment'], 
            'url'
        )
        client_secret: str = _get_auth_credentials(
            params['TS_environment'], 
            'client_secret'
        )
        data = {
            'grant_type': 'client_credentials', 
            'client_id': 'pmo-integration', 
            'client_secret': client_secret 
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        req = requests.post(auth_url, data=data, headers=headers)
        response = req.json()
        token_duration = timedelta(seconds=response['expires_in'] - 30)
        token_info = {
            "token": response['access_token'], 
            "expires_at": datetime.now() + token_duration}
    except Exception as e:
        log.error('[Custom] Getting auth token. Failed')
        log.error(e)
        sys.exit(1)
    else:
        log.info('[Custom] Getting auth token. Success.')
        return token_info
        
#* -------------------------------------------------------------------------- *#
        
def _get_auth_credentials(environment: str, type: str):
    """
    Get ISSO url and client_secrets from Vault.
    
    Parameters:
        - environment: str - rms environment (prod, test or dev)
        - type: str - type of credential (url or client_secret)
    
    Return:
        - result: str - ISSO url or client_secret
    """
    try:
        from airflow.hooks.base_hook import BaseHook
        log.info(f'[Custom] Getting auth {type}...')
        if environment == 'prod':
            secrets = BaseHook.get_connection('v_isso_api')
        else:
            secrets = BaseHook.get_connection('v_isso_dev_api')
        if type == 'client_secret':
            result = secrets.password
        else:
            result = secrets.host  
    except Exception as e:
        log.error(f'[Custom] Getting auth {type}. Failed.')
        log.error(e)
        sys.exit(1)
    else:
        log.info(f'[Custom] Getting auth {type}. Success.')
        return result 