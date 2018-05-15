FROM anapsix/alpine-java:8_jdk

COPY . /notifications-monitor-sources

# installing sbt
RUN wget -O - https://piccolo.link/sbt-1.1.5.tgz \
  | gunzip \
  | tar -x -C /usr/local
ENV PATH="/usr/local/sbt/bin:${PATH}"

RUN cd /notifications-monitor-sources \
  && sbt stage \
  && mv target/universal/stage /notifications-monitor \
  && cd / \
  && rm -rf /notifications-monitor-sources \
  && rm -rf /root/.m2/* \
  && rm -rf /root/.ivy2/*

WORKDIR /notifications-monitor/bin

CMD [ "./notifications-monitor" ]
