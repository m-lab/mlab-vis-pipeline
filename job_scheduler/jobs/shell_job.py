'''
A job to run executable programs via the commandline
'''

import os
import pwd
import subprocess
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

    def demote(self, user_uid, user_gid):
        def result():
            report_ids('starting demotion')
            os.setgid(user_gid)
            os.setuid(user_uid)
            report_ids('finished demotion')

    def run(self, *args, **kwargs):
        '''
        Run the job
        '''
        pw_record = pwd.getpwnam("root")
        user_name = pw_record.pw_name
        user_home_dir = pw_record.pw_dir
        user_uid = pw_record.pw_uid
        user_gid = pw_record.pw_gid

        env = os.environ.copy()
        env['HOME'] = user_home_dir
        env['LOGNAME'] = user_name
        env['USER'] = user_name

        p = subprocess.Popen(
            args, preexec_fn=self.demote(user_uid, user_gid),
            cwd=user_home_dir, env=env, stdout=PIPE, stderr=PIPE
        )
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