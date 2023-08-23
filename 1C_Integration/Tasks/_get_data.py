def get_data(date_from: str, date_to: str, params: dict) -> None:
    try:
        print('[Custom] Loading data...')
        
        # Import required unit functions
        from Utils._get_secrets import get_secrets
        
        # Import required libraries
        import requests as rqst
        import pandas as pd
    
        # Get api url and auth token
        api_secrets = get_secrets(params['1C_UH_environment'])
        api_url = api_secrets.host
        api_auth = api_secrets.login + ' ' + api_secrets.password

        # Define post data
        post_data: str = f'''
            <soap:Envelope 
                xmlns:soap="http://www.w3.org/2003/05/soap-envelope" 
                xmlns:ns="http://uh_dwh.mts-digital.corp/1.0.0.1">
                <soap:Header/>
                    <soap:Body>
                        <ns:AccountingAnalysts>
                        <ns:DataBegin>{date_from}</ns:DataBegin>
                        <ns:DataEnd>{date_to}</ns:DataEnd>
                        </ns:AccountingAnalysts>
                    </soap:Body>
            </soap:Envelope>
        '''

        # Define API request headers
        headers: dict = {
            'Authorization': api_auth
        }

        # Execute request and get response
        response = rqst.post(api_url, data=post_data, headers=headers)

        if response.status_code == 200:
            
            # Get text of response
            raw_xml: str = response.text

            # Replace < and > with thier special symbols
            formatted_xml: str = raw_xml.replace('&lt;', '<').replace('&gt;', '>')

            # Find start and end position of object list
            start: int = formatted_xml.find('<JournalEntries>') 
            end: int = formatted_xml.find('</JournalEntries>')

            # Get length of object list closing tag
            lenght: int = len('</JournalEntries>')
 
            # Truncate xml with only object list
            xml: str = formatted_xml[start : end + lenght] 

            # Create dataframe from given xml
            dataframe: pd.DataFrame = pd.read_xml(xml)

            # Delete last column
            dataframe: pd.DataFrame = dataframe.drop('REFERENCE2GUID', axis=1)
        
        else:
            raise Exception(response.status_code)
        
    except Exception as e:
        print('[Custom] Loading data. Failed.')
        print(e)
        raise e
    else:
        print('[Custom] Loading data. .')
        return dataframe
