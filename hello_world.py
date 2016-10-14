from time import sleep

import luigi


class HelloWorldTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('results/hello_world.txt')

    def requires(self):
        return [
            HelloTask(),
            WorldTask(),
        ]

    def run(self):
        sleep(60)
        with open('results/hello.txt', 'r') as hello_file:
            hello_contents = hello_file.read()

        with open('results/world.txt', 'r') as world_file:
            world_contents = world_file.read()

        with open('results/hello_world.txt', 'w') as hello_world_file:
            hello_world_file.write(hello_contents + ' ' + world_contents + '!')


class HelloTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('results/hello.txt')

    def requires(self):
        return []

    def run(self):
        sleep(60)
        with open('results/hello.txt', 'w') as hello_file:
            hello_file.write('Hello')
            hello_file.close()


class WorldTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('results/world.txt')

    def requires(self):
        return []

    def run(self):
        sleep(60)
        with open('results/world.txt', 'w') as hello_file:
            hello_file.write('World')
            hello_file.close()


if __name__ == '__main__':
    luigi.run()
