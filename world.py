import luigi
from time import sleep

class WorldTask(luigi.Task):

    def run(self):
        sleep(30)
        with open('world.txt', 'w') as world_file:
            world_file.write('World')
            world_file.close()

    def output(self):
        return luigi.LocalTarget('world.txt')

        
if __name__ == '__main__':
    luigi.run()
