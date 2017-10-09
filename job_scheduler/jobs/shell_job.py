'''
A job to run executable programs via the commandline
'''

from subprocess import call
from ndscheduler import job
from subprocess import Popen, PIPE

class ShellJob(job.JobBase):

    @classmethod
    def meta_info(cls):
        print('%s.%s' % (cls.__module__, cls.__name__))
        return {
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': ('This will run an executable program. You can specify '
                      'as many arguments as you want. This job will pass these '
                      'arguments to the program in order.'),
            'arguments': [
                {'type': 'string', 'description': 'Executable path'}
            ],

            'example_arguments': [
                "/usr/local/my_program", "--file",
                "/tmp/abc", "--mode", "safe"]
        }

    def run(self, *args, **kwargs):
        '''
        Run the job
        '''
        p = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        response = {
            'returncode': p.returncode,
            'output': output
        }
        if (err):
            response['err'] = err

        return response

if __name__ == "__main__":
    # You can easily test this job here
    job = ShellJob.create_test_instance()
    job.run('ls', '-l')