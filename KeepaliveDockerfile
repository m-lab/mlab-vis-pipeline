FROM google/cloud-sdk
FROM gcr.io/google-appengine/openjdk:8

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get install -y nginx openssh-server git-core openssh-client curl \
    nano build-essential openssl zlib1g zlib1g-dev libssl-dev libyaml-dev \
    libxml2-dev libxslt-dev autoconf libc6-dev ncurses-dev automake \
    libtool pkg-config python python-dev python-pip wget supervisor

# Install the Google Cloud SDK from the latest
RUN apt-get install unzip && \
  curl -O https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.zip && \
  unzip google-cloud-sdk.zip -d /google/ && \
  rm google-cloud-sdk.zip && \
  echo PATH=$PATH:/google/google-cloud-sdk/bin >> /etc/profile && \
  /google/google-cloud-sdk/install.sh \
      --rc-path=/etc/bash.bashrc \
      --disable-installation-options && \
  /google/google-cloud-sdk/bin/gcloud config set \
      component_manager/disable_update_check True && \
      . /etc/profile

# setup nginx
RUN mkdir /mlab-vis-pipeline
ADD deploy-build/ /mlab-vis-pipeline/deploy-build
WORKDIR /mlab-vis-pipeline
COPY deploy-build/nginx.conf /etc/nginx/nginx.conf
RUN service nginx start
EXPOSE 80

ENV DOCKER_HOST unix:///var/run/docker.sock

# setup supervisord
RUN touch /var/log/supervisor.log

# setup jobs
ADD job_scheduler/ /mlab-vis-pipeline/job_scheduler
COPY deploy-build/scheduler_jobs.json /mlab-vis-pipeline/job_scheduler/
WORKDIR /mlab-vis-pipeline/job_scheduler/
RUN pip install -r requirements.txt
RUN touch /var/log/jobs.log

WORKDIR /mlab-vis-pipeline
COPY deploy-build/supervisord.conf /etc/supervisor/supervisord.conf
ADD tools/ /mlab-vis-pipeline/tools
ADD environments/ /mlab-vis-pipeline/environments
ADD *.sh /mlab-vis-pipeline/

CMD ["/bin/sh", "-c", "service nginx restart && `which supervisord` --configuration /etc/supervisor/supervisord.conf"]
