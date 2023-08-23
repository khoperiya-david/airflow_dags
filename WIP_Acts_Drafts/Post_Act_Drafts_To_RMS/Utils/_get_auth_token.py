def get_auth_token(credentials: dict) -> str:
    """
    Get auth token from ISSO.
    """
    try:
        
        print('[Custom] Getting auth token...')
        
        import requests
        
        data = {
            'grant_type': 'client_credentials', 
            'client_id': 'pmo-integration', 
            'client_secret': credentials['client_secret']
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        
        req = requests.post(credentials['url'], data=data, headers=headers)
        
        response = req.json()
        
        token = response['access_token']
        
    except Exception as e:
        print('[Custom] Getting auth token. Failed')
        print(e)
        raise e
    else:
        print('[Custom] Getting auth token. Success.')
        return token