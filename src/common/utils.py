

class Utils:
    '''
    Common functions used throughout the modules 

    '''
    @staticmethod
    def remove_trailing_slash(path: str):
        return path.rstrip("/")