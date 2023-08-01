"""
Gather all various operations that can be re-used.
"""


def convert_listtuples_tuple(self, result):
    '''
    Read: Convert "list of tuples" returned by oracle query to "tuple"
    Example: convert_listtuples_tuple(result from oracle cursor)
    '''
    return next(zip(*result))