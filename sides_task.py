import luigi
from time import sleep

class SideTask(luigi.Task):

    def run(self):

        sleep(10)

        print('Finished running ')
