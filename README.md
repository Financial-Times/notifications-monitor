# notifications-montior

An app for analyzing outages and stability of notifications-push.

## Install and run

You will need:

* Java 8 installed
* [sbt](http://www.scala-sbt.org/download.html) installed
  - OS X: `brew install sbt` or
  - Linux: `curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo` and `sudo yum install sbt`

To run:

`sbt run`

To develop:

* `sbt compile`

* `sbt test`

To create jars and executables for production use:

`sbt stage`

Then run the generated executable script from `target/universal/stage/bin/notifications-monitor`

Or use the init.d service that's should be installed on the production machine:

`sudo /etc/init.d/notifications-monitor start`
`sudo /etc/init.d/notifications-monitor stop`