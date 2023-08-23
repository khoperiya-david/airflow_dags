import pandas as pd
from typing import Dict, List
import requests

def post_data_to_rms(json: str, params: Dict) -> int:
    """
    Post JSON format data to RMS API.
    """
    try:
        
        print('[Custom] Posting data to RMS...')
        
        from Utils._get_isso_auth_credentials import get_isso_auth_credentials
        from Utils._get_auth_token import get_auth_token
        
        # Get auth credentials for ISSO token
        isso_auth_credentials: dict = get_isso_auth_credentials(params)
        
        # Get auth token
        auth_token = get_auth_token(isso_auth_credentials)
        
        # Post json
        response_status: int = _post_data(
            json, 
            params['rms_environment'], 
            auth_token
        )
        
    except Exception as e:
        print('[Custom] Posting data to RMS. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Posting data to RMS. Success.')
        return response_status
        
#* -------------------------------------------------------------------------- *#


def _post_data(data: str, environment: str, token: str) -> None:
    """
    Post given data to RMS API.
    """
    try:
        
        print('[Custom] Posting data...')
        
        if environment == 'prod':
            url: str = 'https://rms.mtsit.com/api/projects/acts/wip'
        elif environment == 'test' :
            url: str = 'https://test.rms.mtsit.com/api/projects/acts/wip'
        else:
            url: str = 'https://dev.rms.mtsit.com/api/projects/acts/wip'
        
        headers: dict = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token
        }
        
        response = requests.post(url, json=data, headers=headers)
        
        response_status: int = response.status_code
        
    except Exception as e:
        print('[Custom] Posting data. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Posting data. Success.')
        return response_status