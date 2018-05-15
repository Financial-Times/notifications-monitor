# notifications-montior

An app for analyzing outages and stability of notifications-push.

## Running with Docker:

```
docker build -t notifications-monitor:local .

docker run
  --env DELIVERY_BASIC_AUTH_USERNAME=yourusername
  --env DELIVERY_BASIC_AUTH_PASSWORD=abc
  --env DELIVERY_API_KEY=abcdef
  notifications-monitor:local
```

The current config is set for staging-delivery, should be configurable in `application.conf`,
later I should move all parameters to come from environment variables.

## Running with Java:

You will need:

* Java 8
* [sbt](http://www.scala-sbt.org/download.html) sbt is a build tool just like maven. It doesn't mean the project is in Scala, the sources are 99.99% clean Java.
  - OS X: `brew install sbt@1` or
  - Linux: `curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo` and `sudo yum install sbt`

* `sbt compile`
* `sbt test`
* `sbt run`


## App logic explained

todo
