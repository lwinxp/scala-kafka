# Scala Kafka

## Implementation Notes
1. Open new tab/window 1
2. docker run --rm -d --name zookeeper-server -p 2181:2181 -p 2888:2888 -p 3888:3888 -p 8080:8080 -e ALLOW_ANONYMOUS_LOGIN=yes -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper
3. docker run --rm -d --name kafka-server -p 9092:9092 -e ALLOW_PLAINTEXT_LISTENER=yes -e KAFKA_ZOOKEEPER_CONNECT=192.168.1.176:2181 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 confluentinc/cp-kafka
4. docker run -d --name postgres -e POSTGRESQL_USER=scalauser -e POSTGRESQL_PASSWORD=abc123 -e POSTGRESQL_DATABASE=myapp -p 5432:5432 centos/postgresql-12-centos8
5. docker exec -it myapp /bin/bash 
6. psql -U postgres -d postgres
7. ALTER USER postgres WITH PASSWORD 'abc123';
8. Open new tab/window 2
9. docker exec -it myapp /bin/bash
10. kafka-console-consumer --bootstrap-server localhost:9092 --topic events
11. Open new tab/window 3
12. docker exec -it kafka-server /bin/bash
13. kafka-console-consumer --bootstrap-server localhost:9092 --topic bee-counts
14. Open new tab/window 4
15. docker exec -it kafka-server /bin/bash
16. kafka-console-consumer --bootstrap-server localhost:9092 --topic long-distance-travellers
17. In GenerateBeeFlight.scala, W and H arguments can be set. Can consider to set W=1, H=1 and no. of bees = 2 for ease of testing and observation. Note the position of "NOTE" comment showing where the W, H arguments can be set.
18. In CountBeeLandings.scala, T argument can be set. Note the position of "NOTE" comment showing where the T argument can be set.
19. In LongDistanceFlyers.scala, K argument can be set. Note the position of "NOTE" comment showing where the K argument can be set.
20. Open src/main/scala/Main.scala
21. Run Main.scala
22. Observe Kafka output on Window 2 for GenerateBeeFlight.scala (screenshot2.png)
23. Observe Kafka output on Window 3 for CountBeeLandings.scala, output only appears at the end of time window for all squares, according to readme instructions. There might be some processing lag at the start hence it is best to let the program run a bit to stabilise the T interval output (screenshot3.png)
24. Observe Kafka output on Window 4 for LongDistanceFlyers.scala, output is independent of time window and appears once a bee guid has met criteria, according to readme instructions (screenshot4.png)
25. Observe table created in Window 1 with command \d
26. Observe bee guid saved in database with command SELECT * FROM longdistancetravellers; after sufficient number of records output relative to K (screenshot1.png)
27. Run TestCountBeeLandings.scala where CountBeeLandings is tested with expected input where the count of x,y coordinates in input records corresponds with the count of x,y coordinates in the expected output records (screenshot5.png)
28. Run TestLongDistanceFlyers.scala where LongDistanceFlyers is tested with expected input where records of bee guid is reflected in the guid in the expected output once K squares have been exceeded (screenshot6.png)

## Problem Statement

Consider the following experiment. A bee population (10 - 100 individuals) is fitted
with sensors and confined in a rectangular area of size `W x H` length units. The bees fly from
(say) flower to flower (we consider the flowers to have zero height), 
alternating between periods of flight and periods of being
at rest. The sensors are able to sense when the bee has landed and
is at rest, and sends a wireless signal to a central receiver. The sensors cannot
sense the resumption of flight, since that event is not of interest to us.

The sensor signal emitted when a bee lands
is converted by the central receiver into a piece of data with the
following attributes:
  * bee's id (as a string -- preferably a GUID)
  * the current timestamp, expressed as *epoch* time (i.e. number of seconds since
     January 1, 1970, 00:00 GMT)
  * `x` and `y` coordinates of the landing spot

To define the objectives of this problem, we will introduce a few conventions:

  * The rectangular area of confinement is divided into squares of `1 x 1` each. 
    Each square is identified by the pair of coordinates corresponding to its
    bottom-left corner.

  * Moreover, time is divided into `T`-second intervals (or *windows*); 
    each window ends on a timestamp that is a multiple of `T`.
    For instance, if `T` is 900 (i.e. 15 min), then there would be a window ending
    at timestamp 1648054800, another one ending at 1648055700 (= 1648054800 + 900), 
    and so on.

  * A bee that has landed on more than `K` squares is a *long-distance traveller*.


Considering `W`, `H`, `T` and `K` as inputs to our problem, the objectives of
this project are as follows:

  * Compute the number of bees that have landed in each square during each time interval.
    At the end of each time window, publish the number of bees for each square on a given
    topic (see the implementation section).
    The computation has to appear to be performed in *real-time*, that is, the output 
    corresponding to each time window has to happen as soon as possible after the window's 
    expiration time.

  * Detect the long-distance travellers and publish their IDs on a topic. Also save these
    IDs into a table in a database.

## Implementation

Assume that the central receiver publishes the landing
event data on a Kafka topic called `events`. Each published
event has the bee id, the landing timestamp (rounded down to the nearest second), 
and the coordinates of the landing point, all in CSV format.

  * **Task 1:** simulate the bees' landing events and publish them on the `events` topic on Kafka.

    * Start with a timestamp close/equal to the current time.
    * Generate random bee ID, `x` and `y` coordinates, within the given limits.
    * Publish the event (ID, timestamp, `x`, `y`) on the `events` topic.
    * Advance the timestamp by a random amount of time (possibly 0).
    * Repeat from second step.
    * Let this process run forever.
    * Place the code into the `GenerateBeeFlight.scala` file.

  * **Task 2:** using the `KStreams` DSL, implement a pipeline that computes
    the number of bee landings in each square, for each time window.

    * You may find the concept of *window* and *window aggregation* useful here.
    * You may define auxiliary topics if you feel they would be helpful.
    * Publish your output counts to the topic `bee-counts`.
    * Place your code into the `CountBeeLandings.scala`.

  * **Task 3:** using the `KStreams` DSL, implement a pipeline that detects
    long-distance travellers.

    * Publish long-distance travellers on the `long-distance-travellers` topic.
    * You must publish such travellers exactly once, as soon as they are detected.
    * Place your code into the `LongDistanceFlyers.scala` file.

  * **Task 4:** save all the long-distance travellers into the table `longdistancetravellers`
    in a Postgres DB.

    * Place your code into the `SaveLongDistanceFlyers.scala` file.
