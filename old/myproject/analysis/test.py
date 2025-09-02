

from myproject.src import data_handler
from myproject.src import plots
from myproject.src.linear_adjuster import LinearAdjuster




def run():
    print('in /analysis/test.py')
    data_handler.read_local_data()
    la = LinearAdjuster()
    plots.plot()


if __name__ == '__main__':
    run()
