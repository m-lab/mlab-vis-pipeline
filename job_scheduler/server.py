"""Run the scheduler process."""

from ndscheduler.server import server
import os
import signal
import logging
import tornado.wsgi
import tornado.httpserver
import wsgiref.simple_server
import json

from tools.utils import read_json, write_csv
from ndscheduler import settings
from ndscheduler.core import scheduler_manager

logger = logging.getLogger(__name__)

class SchedulerServer(server.SchedulerServer):

    def post_scheduler_start(self):

        # New user experience! Make sure we have at least 1 job to demo!
        jobs = self.scheduler_manager.get_jobs()

        # Remove all jobs, so we can ensure the
        # right default jobs
        for job in jobs:
            self.scheduler_manager.remove_job(job.id)

        # Add keep alive ping prometheus
        api_mode = 'sandbox'
        if 'API_MODE' in os.environ:
            api_mode = os.environ['API_MODE']

        # Read jobs from scheduler_jobs.json and add them.
        current_directory = os.path.abspath(
            os.path.join(os.path.dirname(os.path.realpath(__file__))))
        jobs_file = os.path.join(current_directory, "scheduler_jobs.json")

        if os.path.isfile(jobs_file):
            jobs = read_json(jobs_file)
            for job in jobs["jobs"]:
                self.scheduler_manager.add_job(
                    job_class_string=job['job_class_string'],
                    name=job['name'],
                    pub_args=job['args'],
                    hour=job['hour'] if 'hour' in job else "*",
                    minute=job['minute'] if 'minute' in job else "*",
                    day=job['day'] if 'day' in job else "*",
                    month=job['month'] if 'month' in job else "*",
                    day_of_week=job['day_of_week'] if 'day_of_week' in job else "*")
        else:
            self.scheduler_manager.add_job(
                job_class_string='job_scheduler.jobs.shell_job.ShellJob',
                name='Keep alive',
                pub_args=['/bin/bash', '/mlab-vis-pipeline/keepalive.sh',
                        '-m', api_mode],
                hour='*')

if __name__ == "__main__":
    SchedulerServer.run()