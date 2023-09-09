
from . import data_handler
from . import metrics


class LinearAdjuster:
    def __init__(self):
        print('initiating LinearAdjuster')
        data_handler.return_scorebins()
        metrics.pd_by_score()




