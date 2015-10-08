# Performance tests for Neon

These tests are based on how the Neon GTD interacts with the server.

## Running

Performance tests are executed via [gatling maven plugin](https://github.com/excilys/gatling/wiki/Maven-plugin).
Running performance tests is as simple as following:

```mvn clean test```

You can pass custom cluster url via system property:

```mvn clean test -Dcluster.url=https://myserver.gooddata.com```

### Jenkins
There is a [jenkins job](https://ci.intgdc.com/job/MSF-Performance-Tests/) for automatic execution.
[ConvertGatlingToXunit.groovy](ConvertGatlingToXunit.groovy) is used for generating XUnit xml report from gatling.log



