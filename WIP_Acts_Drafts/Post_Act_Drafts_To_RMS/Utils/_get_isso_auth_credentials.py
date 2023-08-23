def get_isso_auth_credentials(params: dict) -> dict:
    """
    Get ISSO url and client_secrets from Vault.
    """
    try:
        
        print(f'[Custom] Getting auth credentials...')
        
        from airflow.hooks.base_hook import BaseHook
        
        if params['rms_environment'] == 'prod':
            secrets = BaseHook.get_connection('v_isso_api')
        else:
            secrets = BaseHook.get_connection('v_isso_dev_api')
            
        result: dict = {
            'client_secret': secrets.password,
            'url': secrets.host
        }
            
    except Exception as e:
        print(f'[Custom] Getting auth credentials. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Getting auth credentials. Success.')
        return result 