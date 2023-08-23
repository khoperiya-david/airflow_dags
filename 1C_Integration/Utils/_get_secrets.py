def get_secrets(environment: str) -> str:
    try:

        print(f'[Custom] Getting secrets for {environment}...')

        from airflow.hooks.base_hook import BaseHook

        if environment == 'prod':
            secrets = BaseHook.get_connection('v_web_api_1CUH')
        else:
            secrets = BaseHook.get_connection('v_web_api_1CUH')
         
    except Exception as e:
        print(f'[Custom] Getting secrets for {environment}. Failed.')
        print(e)
        raise e
    else:
        print(f'[Custom] Getting secrets for {environment}. Success.')
        return secrets 