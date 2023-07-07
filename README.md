# kafka_connector

A thin rust wrapper around the REST API of kafka's [connector](https://docs.confluent.io/platform/current/connect/references/restapi.html).
This was created just to make another project of mine work, so it's not production ready and needs more fine tuning to be a real library.
Nevertheless it has passing tests (they require a running connector and a MongoDB as well as the corresponding plugin) and does mostly what it is supposed to do.
Feel free to open a PR, if you have more time than me to extend it.

**Author** N.J. Lohmann
