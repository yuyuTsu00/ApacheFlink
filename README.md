# Apache Flink Local Learning Environment

This repository is intended for learning and practicing Apache Flink locally using:

* Apache Flink 1.20.4
* Apache Kafka (KRaft mode)
* Kafdrop (Kafka UI)
* Java 17
* Maven

The setup provides a complete local environment for running Flink jobs, producing Kafka events, exploring topics, and practicing stateful stream processing concepts.

### note:
all the below commands are for mac and aimed for path /Desktop/learning/flink (change according to your need)

# Architecture

```text
Producer
   |
   v
Kafka Topic
   |
   v
Apache Flink Job
   |
   +--> Logs
   +--> Files
   +--> Checkpoints
   +--> Savepoints

Kafdrop UI
   |
   +--> View Topics
   +--> View Messages
```

---

# Prerequisites

Install the following:

* Java 17
* Maven 3.9+
* Git
* macOS/Linux terminal

Verify:

```bash
java -version
mvn -version
git --version
```

---

# Directory Structure

Create a dedicated learning workspace:

```bash
mkdir -p ~/tools
mkdir -p ~/Desktop/learning
```

---

# Install Apache Flink

## Download Flink

```bash
cd ~

curl -LO https://archive.apache.org/dist/flink/flink-1.20.4/flink-1.20.4-bin-scala_2.12.tgz
curl -LO https://archive.apache.org/dist/flink/flink-1.20.4/flink-1.20.4-bin-scala_2.12.tgz.sha512
```

## Verify Download

```bash
shasum -a 512 -c flink-1.20.4-bin-scala_2.12.tgz.sha512
```

Expected output:

```text
OK
```

## Extract

```bash
tar -xzf flink-1.20.4-bin-scala_2.12.tgz

mv ~/flink-1.20.4 ~/tools/
```

## Create Symlink

```bash
rm -rf ~/Desktop/learning/flink

ln -sfn ~/tools/flink-1.20.4 ~/Desktop/learning/flink
```

## Configure Environment Variables

Add to `~/.zshrc`:

```bash
export FLINK_HOME="$HOME/Desktop/learning/flink"
export PATH="$FLINK_HOME/bin:$PATH"
```

Reload:

```bash
source ~/.zshrc
```

## Verify Installation

```bash
flink --version
which flink
```

## Start/Stop Flink

```bash
$FLINK_HOME/bin/start-cluster.sh
$FLINK_HOME/bin/stop-cluster.sh
```
### Create startup alias

```bash
alias flink-start='cd "$FLINK_HOME" && ./bin/start-cluster.sh'
alias flink-stop='cd "$FLINK_HOME" && ./bin/stop-cluster.sh'
```

## Flink Dashboard

Open:

```text
http://localhost:8081
```

---

# Install Apache Kafka

Kafka is configured in single-node KRaft mode (ZooKeeper-free).

## Download Kafka

```bash
cd ~/Desktop/learning

curl -LO https://archive.apache.org/dist/kafka/4.3.0/kafka_2.13-4.3.0.tgz
```

## Verify Download

```bash
file kafka_2.13-4.3.0.tgz
ls -lh kafka_2.13-4.3.0.tgz
```

The file should be approximately 120 MB and recognized as gzip compressed data.

## Extract

```bash
tar -xzf kafka_2.13-4.3.0.tgz

ln -sfn ~/Desktop/learning/kafka_2.13-4.3.0 ~/Desktop/learning/kafka
```

---

## Configure Kafka

Create directories:

```bash
mkdir -p ~/Desktop/learning/kafka/config
mkdir -p ~/Desktop/learning/kafka/data
```

Copy default configuration:

```bash
cp ~/Desktop/learning/kafka/config/kraft/server.properties ~/Desktop/learning/kafka/config/server.properties
```

Update `log.dirs` in `server.properties`:

```properties
log.dirs=/Users/<your-user>/Desktop/learning/kafka/data
```

---

## Format Kafka Storage

Generate cluster ID:

```bash
CLUSTER_ID=$(~/Desktop/learning/kafka/bin/kafka-storage.sh random-uuid)

echo $CLUSTER_ID
```

Format storage:

```bash
~/Desktop/learning/kafka/bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c ~/Desktop/learning/kafka/config/server.properties \
  --standalone
```

---

## Configure Environment Variables

Add to `~/.zshrc`:

```bash
export KAFKA_HOME="$HOME/Desktop/learning/kafka"
export PATH="$KAFKA_HOME/bin:$PATH"
```

Reload:

```bash
source ~/.zshrc
```

---

## Start/Stop Kafka

```bash
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
$KAFKA_HOME/bin/kafka-server-stop.sh $KAFKA_HOME/config/server.properties
```

### Create startup alias

```bash
alias kafka-start='"$KAFKA_HOME"/bin/kafka-server-start.sh "$KAFKA_HOME"/config/server.properties'
alias kafka-stop='"$KAFKA_HOME"/bin/kafka-server-stop.sh "$KAFKA_HOME"/config/server.properties'
```

---

## Verify Kafka

Open another terminal:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

# Install Kafdrop

Kafdrop provides a web UI for Kafka.

## Download

```bash
mkdir -p ~/Desktop/learning/kafdrop

cd ~/Desktop/learning/kafdrop

curl -LO https://github.com/obsidiandynamics/kafdrop/releases/download/4.0.2/kafdrop-4.0.2.jar
```

---

## Create Startup Alias

Add to `~/.zshrc`:

```bash
alias kafdrop-start='JAVA_HOME=$(/usr/libexec/java_home -v 17) java \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
-jar $HOME/Desktop/learning/kafdrop/kafdrop-4.0.2.jar \
--kafka.brokerConnect=localhost:9092 \
--server.port=9001 \
--management.server.port=9001'
```

Reload:

```bash
source ~/.zshrc
```

---

## Start Kafdrop

```bash
kafdrop-start
```

## Kafdrop Dashboard

```text
http://localhost:9001
```

---

# Build the Project

```bash
mvn clean package
```

Expected output:

```text
target/flink-learning-1.0.jar
```

---

# Start Test Producer

Run Kafka producer:

```bash
java -cp \
"$FLINK_HOME/lib/kafka-clients-3.4.0.jar:target/flink-learning-1.0.jar" \
com.learn.flink.producer.TransactionProducer
```

---

# Lab 1 - Kafka to Flink

Submit job:

```bash
flink run -c com.learn.flink.lab01.KafkaToLogJob target/flink-learning-1.0.jar
```

Verify output:

```bash
ls -R /tmp/flink-output/lab01

tail -f /tmp/flink-output/lab01/*/*
```

Cancel job:

```bash
flink cancel <JOB_ID>
```

---

# Lab 2 - Tumbling Windows

Submit:

```bash
flink run -c com.learn.flink.lab02.TumblingWindowJob target/flink-learning-1.0.jar
```

Monitor:

```bash
ls /tmp/flink-output/lab02

tail -f $FLINK_HOME/log/*taskexecutor*.out
```

---

# Lab 3 - Checkpointing & Recovery

This demonstrates Flink state recovery.

## Submit Job

```bash
flink run -c com.learn.flink.lab03.CheckpointStatefulJob target/flink-learning-1.0.jar
```

## List Running Jobs

```bash
flink list
```

Copy the Job ID.

## View Checkpoints

```bash
ls -R /tmp/flink-checkpoints
```

## Create Savepoint

```bash
flink savepoint <JOB_ID> file:///tmp/flink-savepoints
```

## Cancel Job

```bash
flink cancel <JOB_ID>
```

## Restore Job

```bash
flink run \
-s file:///tmp/flink-savepoints/<savepoint-folder> \
-c com.learn.flink.lab03.CheckpointStatefulJob \
target/flink-learning-1.0.jar
```

Result:

State continues from the savepoint instead of restarting from zero.

---

# Lab 4 - Late Data Handling

Submit:

```bash
flink run -c com.learn.flink.lab04.LateDataSideOutputJob target/flink-learning-1.0.jar
```

Monitor:

```bash
tail -f $FLINK_HOME/log/*taskexecutor*.out | grep -E "LATE|ON-TIME"
```

Observe how late events are handled.

---

# Lab 5 - ProcessFunction Timers

Submit:

```bash
flink run -c com.learn.flink.lab05.ProcessFunctionJob target/flink-learning-1.0.jar
```

Monitor logs:

```bash
tail -f $FLINK_HOME/log/*taskexecutor*.out
```

---

# Useful URLs

| Service         | URL                   |
| --------------- | --------------------- |
| Flink Dashboard | http://localhost:8081 |
| Kafdrop         | http://localhost:9001 |
| Kafka Broker    | localhost:9092        |

---

# Why Learn Flink?

Some notable Flink advantages:

* Efficient memory management, flink have its own memory management and not depends on JVM GC 
* Reduced dependence on JVM garbage collection
* Operator chaining optimization
  * Reduced serialization/deserialization overhead
  * Lower network I/O
  * Reduced thread context switching
* Powerful stateful stream processing
* Built-in checkpointing and savepoint support
* Advanced event-time processing and late event handling
* Can store upto TBs of data

  https://www.youtube.com/watch?v=3cg5dABA6mo&list=PLa7VYi0yPIH1UdmQcnUr8lvjbUV8JriK0
