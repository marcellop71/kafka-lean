# kafka-lean

Lean 4 bindings for [Apache Kafka](https://kafka.apache.org/) via [librdkafka](https://github.com/confluentinc/librdkafka). Provides type-safe access to Kafka producers and consumers with both low-level FFI bindings and high-level monadic interfaces.

> ⚠️ **Warning**: this is work in progress, it is still incomplete and it ~~may~~ will contain errors

## AI Assistance Disclosure

Parts of this repository were created with assistance from AI-powered coding tools, specifically Claude by Anthropic. Not all generated code may have been reviewed. Generated code may have been adapted by the author. Design choices, architectural decisions, and final validation were performed independently by the author.

## Features

- **Producer API** - Publish messages to Kafka topics with automatic partitioning
- **Consumer API** - Subscribe to topics and consume messages with group management
- **Message Headers** - Attach key-value metadata to messages for tracing, routing, and correlation
- **Metadata API** - Query cluster metadata, topic info, partition details, and watermark offsets
- **Partition Assignment** - Manual partition assignment, seek, pause/resume operations
- **Transactions** - Exactly-once semantics with transactional producers
- **Monadic interfaces** - `ProducerM` and `ConsumerM` for convenient, composable operations
- **Type-safe messaging** - `ToKafkaMessage` and `FromKafkaMessage` typeclasses for automatic serialization
- **Topic patterns** - Type-safe topic naming with `TopicPattern` for consistent naming conventions
- **Batch production** - Efficient batch message production with `produceBatch`
- **JSON support** - `ToKafkaJson` and `FromKafkaJson` for automatic JSON serialization
- **Type-safe configuration** - Strongly typed configuration management
- **Error handling** - Proper error types with descriptive messages
- **Zero-copy message handling** - Efficient ByteArray-based message payloads

## Requirements

- Lean 4 (see `lean-toolchain`)
- librdkafka (`>= 1.0.0`)
- zlog library (for logging)

### Installation

**Ubuntu/Debian:**
```bash
sudo apt-get install librdkafka-dev libzlog-dev
```

**macOS:**
```bash
brew install librdkafka zlog
```

**From source:**
```bash
git clone https://github.com/confluentinc/librdkafka.git
cd librdkafka
./configure
make && sudo make install
```

## Usage

Add to your `lakefile.lean`:

```lean
require kafkaLean from git
  "https://github.com/your-org/kafka-lean" @ "main"
```

### Quick Start: Producer

```lean
import KafkaLean

open Kafka

def produceMessages : IO Unit := do
  -- Create producer
  match ← Producer.new "localhost:9092" with
  | .error e => IO.println s!"Error: {e}"
  | .ok producer => do
    -- Produce messages
    for i in [:10] do
      let _ ← producer.produceString "my-topic" s!"Message {i}"
      let _ ← producer.poll 0

    -- Wait for delivery
    let _ ← producer.flush 10000
```

### Quick Start: Consumer

```lean
import KafkaLean

open Kafka

def consumeMessages : IO Unit := do
  -- Create consumer with group ID
  match ← Consumer.new "localhost:9092" "my-group" with
  | .error e => IO.println s!"Error: {e}"
  | .ok consumer => do
    -- Subscribe to topic
    let _ ← consumer.subscribeTo "my-topic"

    -- Consume messages
    for _ in [:100] do
      match ← consumer.poll 1000 with
      | none => pure ()
      | some msg =>
        if msg.isOk then
          match msg.payloadString with
          | some s => IO.println s!"Received: {s}"
          | none => pure ()

    -- Cleanup
    let _ ← consumer.commit
    let _ ← consumer.close
```

### Message Headers

Attach metadata to messages for distributed tracing, routing, or correlation:

```lean
import KafkaLean

open Kafka

def produceWithHeaders : IO Unit := do
  match ← Producer.new "localhost:9092" with
  | .error e => IO.println s!"Error: {e}"
  | .ok producer => do
    -- Create headers
    let headers := MessageHeaders.empty
      |>.addString "trace-id" "abc123"
      |>.addString "content-type" "application/json"
      |>.addString "source" "my-service"

    -- Produce with headers
    let _ ← producer.produceString "my-topic" "Hello" "" headers
    let _ ← producer.flush 10000

def consumeWithHeaders : IO Unit := do
  match ← Consumer.new "localhost:9092" "my-group" with
  | .error e => IO.println s!"Error: {e}"
  | .ok consumer => do
    let _ ← consumer.subscribeTo "my-topic"

    -- Poll with headers
    match ← consumer.pollWithHeaders 1000 with
    | none => pure ()
    | some msg =>
      if msg.isOk then
        -- Access headers
        match msg.getHeaderString "trace-id" with
        | some traceId => IO.println s!"Trace ID: {traceId}"
        | none => IO.println "No trace ID"

        IO.println s!"Message has {msg.headers.size} headers"
```

### Metadata API

Query cluster and topic metadata:

```lean
import KafkaLean

open Kafka

def queryMetadata : IO Unit := do
  match ← Consumer.new "localhost:9092" "my-group" with
  | .error e => IO.println s!"Error: {e}"
  | .ok consumer => do
    -- Get cluster metadata
    match ← consumer.metadata true 5000 with
    | .error e => IO.println s!"Metadata error: {e}"
    | .ok meta =>
      IO.println s!"Brokers: {meta.brokerCount}"
      IO.println s!"Topics: {meta.topicNames}"

      for broker in meta.brokers do
        IO.println s!"  Broker {broker.id}: {broker.address}"

    -- Get topic metadata
    match ← consumer.topicMetadata "my-topic" 5000 with
    | .error e => IO.println s!"Topic error: {e}"
    | .ok topic =>
      IO.println s!"Topic {topic.name} has {topic.partitionCount} partitions"

    -- Query watermark offsets
    match ← consumer.queryWatermarkOffsets "my-topic" 0 5000 with
    | .error e => IO.println s!"Offset error: {e}"
    | .ok offsets =>
      IO.println s!"Partition 0: low={offsets.low}, high={offsets.high}"
      IO.println s!"Messages available: {offsets.messageCount}"

    -- Find offset for timestamp
    let timestamp : Int64 := 1700000000000  -- Unix timestamp in ms
    match ← consumer.offsetForTime "my-topic" 0 timestamp 5000 with
    | .error e => IO.println s!"Offset lookup error: {e}"
    | .ok offset => IO.println s!"Offset at timestamp: {offset}"
```

### Partition Assignment and Seek

Manual partition control for replay and custom assignment:

```lean
import KafkaLean

open Kafka

def manualAssignment : IO Unit := do
  match ← Consumer.new "localhost:9092" "my-group" with
  | .error e => IO.println s!"Error: {e}"
  | .ok consumer => do
    -- Manual partition assignment (instead of subscribe)
    let _ ← consumer.assignOne "my-topic" 0

    -- Seek to beginning
    let _ ← consumer.seekToBeginning "my-topic" 0

    -- Or seek to specific offset
    let _ ← consumer.seek "my-topic" 0 1000

    -- Consume messages
    for _ in [:10] do
      match ← consumer.poll 1000 with
      | some msg => IO.println s!"Offset {msg.offset}: {msg.payloadString}"
      | none => pure ()

    -- Pause consumption
    let _ ← consumer.pauseOne "my-topic" 0
    IO.println "Paused"

    -- Resume consumption
    let _ ← consumer.resumeOne "my-topic" 0
    IO.println "Resumed"

    -- Get current assignment
    match ← consumer.assignment with
    | .error e => IO.println s!"Assignment error: {e}"
    | .ok partitions =>
      for p in partitions do
        IO.println s!"Assigned: {p.topic}[{p.partition}] @ {p.offset}"
```

### Transactions (Exactly-Once Semantics)

Use transactional producers for exactly-once delivery:

```lean
import KafkaLean

open Kafka

def transactionalProduction : IO Unit := do
  -- Create transactional producer with unique ID
  match ← TransactionalProducer.new "localhost:9092" "my-txn-id" with
  | .error e => IO.println s!"Error: {e}"
  | .ok producer => do
    -- Initialize transactions (call once)
    match ← producer.initTransactions 30000 with
    | .error e =>
      IO.println s!"Init error: {e.message}"
      if e.isFatal then IO.println "Fatal error - must recreate producer"
    | .ok () =>
      -- Use withTransaction for automatic begin/commit/abort
      match ← producer.withTransaction 30000 do
        let _ ← producer.produceString "topic1" "message1"
        let _ ← producer.produceString "topic2" "message2"
        pure ()
      with
      | .error e =>
        IO.println s!"Transaction error: {e.message}"
        if e.txnRequiresAbort then IO.println "Must abort transaction"
        if e.isRetriable then IO.println "Can retry"
      | .ok () => IO.println "Transaction committed"

def exactlyOnceProcessing : IO Unit := do
  -- For consume-transform-produce with exactly-once semantics
  match ← Consumer.new "localhost:9092" "my-group" with
  | .error _ => pure ()
  | .ok consumer => do
    match ← TransactionalProducer.new "localhost:9092" "processor-txn" with
    | .error _ => pure ()
    | .ok producer => do
      let _ ← producer.initTransactions 30000
      let _ ← consumer.subscribeTo "input-topic"

      -- Get consumer group metadata for offset commits
      match ← consumer.groupMetadata with
      | none => IO.println "Failed to get group metadata"
      | some cgMeta => do
        -- Manual transaction with offset commit
        match ← producer.beginTransaction with
        | .error e => IO.println s!"Begin error: {e.message}"
        | .ok () => do
          -- Consume and produce
          match ← consumer.pollWithHeaders 1000 with
          | none => pure ()
          | some msg =>
            -- Transform and produce
            let _ ← producer.produceString "output-topic" s!"Processed: {msg.payloadString.getD ""}"

            -- Commit consumer offsets as part of transaction
            match ← PartitionList.fromTopicPartitionOffsets #[{
              topic := msg.topic
              partition := msg.partition.toInt32
              offset := (msg.offset + 1).toInt64
            }] with
            | none => pure ()
            | some offsets =>
              let _ ← producer.sendOffsetsToTransaction offsets cgMeta 30000

          -- Commit transaction
          let _ ← producer.commitTransaction 30000
```

### Type-Safe Messaging with Typeclasses

Define your message types with automatic topic routing and serialization:

```lean
import KafkaLean
import Lean.Data.Json

open Kafka

-- Define your message type
structure UserEvent where
  userId : String
  action : String
  timestamp : Nat
  deriving Repr

-- Implement ToJson for serialization
instance : Lean.ToJson UserEvent where
  toJson e := Lean.Json.mkObj [
    ("userId", e.userId),
    ("action", e.action),
    ("timestamp", e.timestamp)
  ]

-- Implement ToKafkaJson for automatic topic routing
instance : ToKafkaJson UserEvent where
  topic := fun e => s!"events:{e.action}"
  key := fun e => some e.userId

-- Now you can produce with automatic serialization and routing
def produceTyped (producer : Producer) (event : UserEvent) : IO Unit := do
  match ← producer.produceTyped event with
  | .ok () => pure ()
  | .error e => IO.println s!"Error: {e.message}"
```

### Topic Patterns

Use `TopicPattern` for consistent topic naming across your application:

```lean
import KafkaLean

open Kafka

-- Use the fluent builder
def logsPattern := TopicBuilder.new
  |>.withPrefix "myapp"
  |>.withSecurityType (.other "logs")
  |>.withDataType (.other "events")
  |>.build  -- myapp:logs:events

-- Custom patterns
def customPattern : TopicPattern := {
  topicPrefix := "myapp"
  securityType := .other "notifications"
  dataType := .other "email"
  separator := "."
}
-- myapp.notifications.email
```

### Batch Production

Efficiently produce multiple messages:

```lean
import KafkaLean

open Kafka

def produceBatch (producer : Producer) (events : Array UserEvent) : IO Unit := do
  let result ← producer.produceBatch events
  IO.println s!"Succeeded: {result.succeeded}, Failed: {result.failed}"
  match result.firstError with
  | some e => IO.println s!"First error: {e.message}"
  | none => pure ()
```

### Typed Consumption

Consume messages with automatic deserialization:

```lean
import KafkaLean

open Kafka

-- Implement FromKafkaJson for deserialization
instance : Lean.FromJson UserEvent where
  fromJson? j := do
    let userId ← j.getObjValAs? String "userId"
    let action ← j.getObjValAs? String "action"
    let timestamp ← j.getObjValAs? Nat "timestamp"
    pure { userId, action, timestamp }

instance : FromKafkaJson UserEvent := {}

-- Consume with automatic deserialization
def consumeTyped (consumer : Consumer) : IO Unit := do
  consumer.forEachTyped 1000 fun (event : UserEvent) => do
    IO.println s!"Event: user={event.userId} action={event.action}"
    pure true  -- continue consuming
```

### Using Monadic Interface

```lean
import KafkaLean

open Kafka

-- Producer monad
def produceWithMonad : IO Unit := do
  match ← ProducerM.withProducer "localhost:9092" [] (do
    for i in [:5] do
      let _ ← ProducerM.produceString "my-topic" s!"Message {i}"
    ProducerM.flush 5000
  ) with
  | .error e => IO.println s!"Error: {e}"
  | .ok _ => IO.println "Done"

-- Consumer monad
def consumeWithMonad : IO Unit := do
  match ← ConsumerM.withConsumer "localhost:9092" "my-group" #["my-topic"] [] (do
    for _ in [:10] do
      match ← ConsumerM.pollValid 1000 with
      | some msg => IO.println s!"{msg.payloadString}"
      | none => pure ()
    ConsumerM.commit
  ) with
  | .error e => IO.println s!"Error: {e}"
  | .ok _ => IO.println "Done"
```

## API Reference

### Message Headers

```lean
-- Header type alias
abbrev MessageHeaders := Array (String × ByteArray)

-- Create and manipulate headers
MessageHeaders.empty : MessageHeaders
MessageHeaders.fromStrings : List (String × String) → MessageHeaders
MessageHeaders.add : MessageHeaders → String → ByteArray → MessageHeaders
MessageHeaders.addString : MessageHeaders → String → String → MessageHeaders
MessageHeaders.get : MessageHeaders → String → Option ByteArray
MessageHeaders.getString : MessageHeaders → String → Option String
MessageHeaders.getAll : MessageHeaders → String → Array ByteArray
MessageHeaders.contains : MessageHeaders → String → Bool
```

### Metadata Types

```lean
-- Broker information
structure BrokerInfo where
  id : UInt32
  host : String
  port : UInt32

-- Partition information
structure PartitionInfo where
  id : UInt32
  error : ErrorCode
  leader : UInt32
  replicas : Array UInt32
  isrs : Array UInt32

-- Topic information
structure TopicInfo where
  name : String
  error : ErrorCode
  partitions : Array PartitionInfo

-- Cluster metadata
structure ClusterMetadata where
  brokers : Array BrokerInfo
  topics : Array TopicInfo
  origBrokerId : UInt32
  origBrokerName : String

-- Watermark offsets
structure WatermarkOffsets where
  low : UInt64
  high : UInt64
```

### Partition Assignment

```lean
-- Topic-partition types
structure TopicPartition where
  topic : String
  partition : Int32

structure TopicPartitionOffset where
  topic : String
  partition : Int32
  offset : Int64

-- Special offset values
def offsetBeginning : Int64  -- Seek to beginning
def offsetEnd : Int64        -- Seek to end
def offsetStored : Int64     -- Use stored offset

-- Consumer partition methods
Consumer.assign : Consumer → PartitionList → IO (KafkaResult Unit)
Consumer.assignPartitions : Consumer → Array TopicPartition → IO (KafkaResult Unit)
Consumer.assignOne : Consumer → String → Int32 → IO (KafkaResult Unit)
Consumer.unassign : Consumer → IO (KafkaResult Unit)
Consumer.assignment : Consumer → IO (Except String (Array TopicPartitionOffset))
Consumer.seek : Consumer → String → Int32 → Int64 → Int32 → IO (KafkaResult Unit)
Consumer.seekToBeginning : Consumer → String → Int32 → IO (KafkaResult Unit)
Consumer.seekToEnd : Consumer → String → Int32 → IO (KafkaResult Unit)
Consumer.pause : Consumer → PartitionList → IO (KafkaResult Unit)
Consumer.pauseOne : Consumer → String → Int32 → IO (KafkaResult Unit)
Consumer.resume : Consumer → PartitionList → IO (KafkaResult Unit)
Consumer.resumeOne : Consumer → String → Int32 → IO (KafkaResult Unit)
```

### Transactions

```lean
-- Transaction error with recovery info
structure TxnError where
  code : ErrorCode
  message : String
  isFatal : Bool          -- Must recreate producer
  isRetriable : Bool      -- Can retry operation
  txnRequiresAbort : Bool -- Must abort transaction first

-- Transactional producer
TransactionalProducer.new : String → String → List (String × String) → IO (Except String TransactionalProducer)
TransactionalProducer.initTransactions : TransactionalProducer → Int32 → IO (TxnResult Unit)
TransactionalProducer.beginTransaction : TransactionalProducer → IO (TxnResult Unit)
TransactionalProducer.commitTransaction : TransactionalProducer → Int32 → IO (TxnResult Unit)
TransactionalProducer.abortTransaction : TransactionalProducer → Int32 → IO (TxnResult Unit)
TransactionalProducer.produce : TransactionalProducer → ProducerMessage → IO (KafkaResult Unit)
TransactionalProducer.sendOffsetsToTransaction : TransactionalProducer → PartitionList → ConsumerGroupMeta → Int32 → IO (TxnResult Unit)
TransactionalProducer.withTransaction : TransactionalProducer → Int32 → IO α → IO (TxnResult α)

-- Consumer group metadata (for exactly-once)
Consumer.groupMetadata : Consumer → IO (Option ConsumerGroupMeta)
ConsumerGroupMeta.fromGroupId : String → IO (Option ConsumerGroupMeta)
```

### Typeclasses

```lean
-- Type-safe message production
class ToKafkaMessage (α : Type) where
  topic : α → String
  key : α → Option String := fun _ => none
  serialize : α → ByteArray

-- JSON-based message production (auto-implements ToKafkaMessage)
class ToKafkaJson (α : Type) [ToJson α] where
  topic : α → String
  key : α → Option String := fun _ => none

-- Type-safe message consumption
class FromKafkaMessage (α : Type) where
  deserialize : ByteArray → Option α

-- JSON-based message consumption
class FromKafkaJson (α : Type) [FromJson α]
```

### Topic Patterns

```lean
-- Topic pattern structure
structure TopicPattern where
  topicPrefix : String := "data"
  securityType : SecurityType := .stock
  dataType : DataType := .trades
  separator : String := ":"

-- Pattern methods
TopicPattern.build : TopicPattern → String
TopicPattern.forSymbol : TopicPattern → String → String
TopicPattern.bulk : TopicPattern → String

-- Fluent builder
TopicBuilder.new : TopicBuilder
TopicBuilder.withPrefix : TopicBuilder → String → TopicBuilder
TopicBuilder.withSecurityType : TopicBuilder → SecurityType → TopicBuilder
TopicBuilder.withDataType : TopicBuilder → DataType → TopicBuilder
TopicBuilder.build : TopicBuilder → String
```

### Configuration

```lean
-- Create configuration for producer
Config.forProducer : String → List (String × String) → IO (Except String Config)

-- Create configuration for consumer
Config.forConsumer : String → String → List (String × String) → IO (Except String Config)

-- Set configuration property
Config.set : Config → String → String → IO (Except String Unit)
```

### Producer

```lean
-- Create producer
Producer.new : String → List (String × String) → IO (Except String Producer)

-- Low-level production
Producer.produce : Producer → ProducerMessage → IO (KafkaResult Unit)
Producer.produceString : Producer → String → String → String → MessageHeaders → IO (KafkaResult Unit)
Producer.produceBytes : Producer → String → ByteArray → ByteArray → MessageHeaders → IO (KafkaResult Unit)
Producer.produceWithHeaders : Producer → String → ByteArray → ByteArray → MessageHeaders → IO (KafkaResult Unit)

-- Type-safe production
Producer.produceTyped [ToKafkaMessage α] : Producer → α → IO (KafkaResult Unit)
Producer.produceJson [ToJson α] : Producer → String → α → Option String → IO (KafkaResult Unit)
Producer.produceBatch [ToKafkaMessage α] : Producer → Array α → IO BatchResult

-- Metadata
Producer.metadata : Producer → Bool → Int32 → IO (Except String ClusterMetadata)
Producer.topicMetadata : Producer → String → Int32 → IO (Except String TopicInfo)

-- Poll and flush
Producer.poll : Producer → Int32 → IO UInt32
Producer.flush : Producer → Int32 → IO (KafkaResult Unit)
```

### Consumer

```lean
-- Create consumer
Consumer.new : String → String → List (String × String) → IO (Except String Consumer)

-- Subscribe
Consumer.subscribe : Consumer → Array String → IO (KafkaResult Unit)
Consumer.subscribeTo : Consumer → String → IO (KafkaResult Unit)

-- Low-level consumption
Consumer.poll : Consumer → Int32 → IO (Option ConsumerMessage)
Consumer.pollValid : Consumer → Int32 → IO (Option ConsumerMessage)
Consumer.pollWithHeaders : Consumer → Int32 → IO (Option ConsumerMessage)
Consumer.pollValidWithHeaders : Consumer → Int32 → IO (Option ConsumerMessage)

-- Type-safe consumption
Consumer.pollTyped [FromKafkaMessage α] : Consumer → Int32 → IO (Option α)
Consumer.forEachTyped [FromKafkaMessage α] : Consumer → Int32 → (α → IO Bool) → IO Unit
Consumer.collectN [FromKafkaMessage α] : Consumer → Nat → Int32 → IO (Array α)

-- Metadata
Consumer.metadata : Consumer → Bool → Int32 → IO (Except String ClusterMetadata)
Consumer.topicMetadata : Consumer → String → Int32 → IO (Except String TopicInfo)
Consumer.queryWatermarkOffsets : Consumer → String → Int32 → Int32 → IO (Except String WatermarkOffsets)
Consumer.getWatermarkOffsets : Consumer → String → Int32 → IO (Except String WatermarkOffsets)
Consumer.offsetForTime : Consumer → String → Int32 → Int64 → Int32 → IO (Except String UInt64)

-- Commit and close
Consumer.commit : Consumer → IO (KafkaResult Unit)
Consumer.commitAsync : Consumer → IO (KafkaResult Unit)
Consumer.close : Consumer → IO (KafkaResult Unit)
```

### Message Types

```lean
-- Producer message
structure ProducerMessage where
  topic : String
  partition : Int32
  key : ByteArray
  payload : ByteArray
  headers : MessageHeaders

-- Consumer message
structure ConsumerMessage where
  topic : String
  partition : UInt32
  offset : UInt64
  key : ByteArray
  payload : ByteArray
  error : ErrorCode
  headers : MessageHeaders

-- Batch result
structure BatchResult where
  succeeded : Nat
  failed : Nat
  firstError : Option KafkaError
```

## Configuration Options

Common producer options:
- `bootstrap.servers` - Kafka broker addresses
- `acks` - Acknowledgment level (`0`, `1`, `all`)
- `retries` - Number of retries
- `batch.size` - Batch size in bytes
- `linger.ms` - Batching delay

Transactional producer options:
- `transactional.id` - Unique transaction ID (required)
- `enable.idempotence` - Enable idempotent producer (auto-enabled with transactions)

Common consumer options:
- `bootstrap.servers` - Kafka broker addresses
- `group.id` - Consumer group ID
- `auto.offset.reset` - Offset reset policy (`earliest`, `latest`)
- `enable.auto.commit` - Auto commit offsets
- `session.timeout.ms` - Session timeout
- `isolation.level` - Transaction isolation (`read_uncommitted`, `read_committed`)

See [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for all options.

## Project Structure

```
kafka-lean/
├── KafkaLean/
│   ├── FFI.lean         # Low-level FFI bindings
│   ├── Error.lean       # Error types and handling
│   ├── Config.lean      # Configuration management
│   ├── Message.lean     # Message types with headers
│   ├── Topic.lean       # Topic patterns and builders
│   ├── Codec.lean       # Serialization codecs
│   ├── Typeclass.lean   # ToKafkaMessage, FromKafkaMessage, etc.
│   ├── Metadata.lean    # Cluster/topic metadata types
│   ├── Transaction.lean # Transactional producer support
│   ├── Producer.lean    # Producer API
│   └── Consumer.lean    # Consumer API
├── KafkaLean.lean       # Main module
├── kafka/
│   └── kafka_shim.c     # C FFI shim
├── Examples/
│   └── Main.lean        # Usage examples
└── lakefile.lean        # Build configuration
```
