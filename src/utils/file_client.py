import itertools

class InputOutputClient(object):

    @staticmethod
    def flatten_json(data, parent_key=''):
        '''flattens a json in list of dicts with single level hierarchy,
        explodes nested lists if any.'''
        if isinstance(data, dict):
            items = []
            for k, v in data.items():
                new_key = f"{parent_key}_{k}" if parent_key else k
                items.append(InputOutputClient.flatten_json(v, new_key))
            return [{k: v for d in item for k, v in d.items()} for item in itertools.product(*items)]

        elif isinstance(data, list):
            flattened = []
            for item in data:
                flattened.extend(InputOutputClient.flatten_json(item, parent_key))
            return flattened

        else:
            # Force all values to string
            return [{parent_key: str(data)}]