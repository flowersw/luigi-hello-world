import datetime
import os

import luigi


class HelloWorldTask(luigi.Task):
    execution_id = luigi.Parameter(default=datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S'))

    def output(self):
        return luigi.LocalTarget(self.hello_world_file_path.format(self.execution_id))

    def requires(self):
        return [
            HelloTask(file_path=self.hello_file_path),
            WorldTask(file_path=self.world_file_path),
        ]

    @property
    def hello_file_path(self):
        return 'results/{}/hello.txt'.format(self.execution_id)

    @property
    def world_file_path(self):
        return 'results/{}/world.txt'.format(self.execution_id)

    @property
    def hello_world_file_path(self):
        return 'results/{}/hello_world.txt'.format(self.execution_id)

    def run(self):
        with open(self.hello_file_path, 'r') as hello_file:
            hello_contents = hello_file.read()

        with open(self.world_file_path, 'r') as world_file:
            world_contents = world_file.read()

        with open(self.hello_world_file_path, 'w') as hello_world_file:
            hello_world_file.write(hello_contents + ' ' + world_contents + '!')


class HelloTask(luigi.Task):
    file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file_path)

    def requires(self):
        return [
            MakeDirectory(directory_path=os.path.dirname(self.file_path)),
        ]

    def run(self):
        with open(self.file_path, 'w') as hello_file:
            hello_file.write('Hello')
            hello_file.close()


class WorldTask(luigi.Task):
    file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file_path)

    def requires(self):
        return [
            MakeDirectory(directory_path=os.path.dirname(self.file_path)),
        ]

    def run(self):
        with open(self.file_path, 'w') as hello_file:
            hello_file.write('World')
            hello_file.close()


class MakeDirectory(luigi.Task):
    directory_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.directory_path)

    def requires(self):
        return []

    def run(self):
        os.makedirs(self.directory_path)

if __name__ == '__main__':
    luigi.run()
