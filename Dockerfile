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

# Setup maven
ARG MAVEN_VERSION=3.3.9
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries
ENV MAVEN_OPTS=-Xss2m

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

# Add this working folder
ADD . /mlab-vis-pipeline
WORKDIR /mlab-vis-pipeline

# Build jar
RUN cd dataflow && mvn package
