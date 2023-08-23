import pandas as pd
import json as js
def generate_json(dataframe: pd.DataFrame) -> str:
    """
    Generate JSON format string with data.
    """
    try:
        
        print('[Custom] Generating json...')
        
        data_json: str = dataframe.to_json(orient="records", force_ascii=False, date_format='iso', date_unit='s')
        data_list: list = js.loads(data_json)
        result_dict: dict = {
            'WipActs': data_list
        }
        json_result: str = js.dumps(result_dict, ensure_ascii=False, indent=4)
        
    except Exception as e:
        print('[Custom] Generating json.Failed')
        print(e)
        raise e
    else:
        print('[Custom] Generating json.Success.')
        return json_result

#* -------------------------------------------------------------------------- *#
