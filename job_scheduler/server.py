"""Run the scheduler process."""

from ndscheduler.server import server
import os
import signal
import logging
import tornado.wsgi
import tornado.httpserver
import wsgiref.simple_server
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

        self.scheduler_manager.add_job(
            job_class_string='job_scheduler.jobs.shell_job.ShellJob',
            name='Keep alive',
            pub_args=['/bin/bash', '/mlab-vis-pipeline/keepalive.sh',
                      '-m', api_mode],
            hour='*')

        # TODO: eventually add pipeline jar here?

    # @classmethod
    # def run(cls):
    #     if not cls.singleton:
    #         signal.signal(signal.SIGINT, cls.signal_handler)

    #         cls.singleton = cls(scheduler_manager.SchedulerManager.get_instance())
    #         cls.singleton.start_scheduler()
    #         # cls.singleton.application.listen(settings.HTTP_PORT, settings.HTTP_ADDRESS)

    #         # container = tornado.wsgi.WSGIContainer(cls.singleton.application)
    #         # http_server = tornado.httpserver.HTTPServer(container)
    #         # http_server.listen(settings.HTTP_PORT)

    #         logger.info('Running server at %s:%d ...' % (settings.HTTP_ADDRESS, settings.HTTP_PORT))
    #         logger.info('*** You can access scheduler web ui at http://localhost:%d'
    #                     ' ***' % settings.HTTP_PORT)

    #         # tornado.ioloop.IOLoop.current().start()

    #         wsgi_app = tornado.wsgi.WSGIAdapter(cls.singleton.application)
    #         server = wsgiref.simple_server.make_server('', settings.HTTP_PORT, wsgi_app)
    #         server.serve_forever()

if __name__ == "__main__":
    SchedulerServer.run()