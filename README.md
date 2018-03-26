# notifications-montior

An app for analyzing outages and stability of notifications-push.

## Install and run

You will need:

* Java 8 installed
* [sbt](http://www.scala-sbt.org/download.html) installed
  - OS X: `brew install sbt` or
  - Linux: `curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo` and `sudo yum install sbt`

## To run:

`sbt run`

## New

`sbt docker:publishLocal`

`docker run -it -p 8080 notifications-monitor:1.0.1-SNAPSHOT`

## To develop (on your machine):

* `sbt compile`

* `sbt test`

## In production:

1. `ssh Your.Name@upp-notifications-monitor.in.ft.com`
2. Become superuser: `sudo su -`
3. Navigate to `/opt/notifications-monitor`
4. `git pull origin master`
5. Stop the running service: `/etc/init.d/notifications-monitor stop`
6. `sbt stage`
7. Start the service: `/etc/init.d/notifications-monitor start`
7. Check: `/etc/init.d/notifications-monitor status` or `ps aux | grep java`

Logs are in `/opt/notifications-monitor/logs`
