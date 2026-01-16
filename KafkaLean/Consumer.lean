/-
  KafkaLean/Consumer.lean - High-level Kafka consumer API
-/

import KafkaLean.FFI
import KafkaLean.Error
import KafkaLean.Config
import KafkaLean.Message
import KafkaLean.Typeclass
import KafkaLean.Metadata
import KafkaLean.Transaction

namespace Kafka

/-- Kafka consumer -/
structure Consumer where
  handle : FFI.Handle
  deriving Nonempty

namespace Consumer

/-- Create a new consumer -/
def create (config : Config) : IO (Except String Consumer) := do
  match ← FFI.kafka_new_consumer config.ptr with
  | .error e => return .error e
  | .ok handle => return .ok { handle }

/-- Create a consumer with broker string and group ID -/
def new (brokers : String) (groupId : String) (props : List (String × String) := []) : IO (Except String Consumer) := do
  match ← Config.forConsumer brokers groupId props with
  | .error e => return .error e
  | .ok config => create config

/-- Subscribe to topics -/
def subscribe (consumer : Consumer) (topics : Array String) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_subscribe consumer.handle topics
  KafkaResult.fromErrorCode err ()

/-- Subscribe to a single topic -/
def subscribeTo (consumer : Consumer) (topic : String) : IO (KafkaResult Unit) :=
  consumer.subscribe #[topic]

/-- Unsubscribe from all topics -/
def unsubscribe (consumer : Consumer) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_unsubscribe consumer.handle
  KafkaResult.fromErrorCode err ()

/-- Poll for a message (without headers for backwards compatibility) -/
def poll (consumer : Consumer) (timeout_ms : Int32 := 1000) : IO (Option ConsumerMessage) := do
  let raw ← FFI.kafka_consumer_poll consumer.handle timeout_ms
  return raw.map ConsumerMessage.fromRaw

/-- Poll for a message with headers -/
def pollWithHeaders (consumer : Consumer) (timeout_ms : Int32 := 1000) : IO (Option ConsumerMessage) := do
  let raw ← FFI.kafka_consumer_poll_with_headers consumer.handle timeout_ms
  return raw.map ConsumerMessage.fromRawWithHeaders

/-- Poll for messages, returning only valid ones -/
def pollValid (consumer : Consumer) (timeout_ms : Int32 := 1000) : IO (Option ConsumerMessage) := do
  match ← consumer.poll timeout_ms with
  | none => return none
  | some msg =>
    if msg.isOk then return some msg
    else return none

/-- Poll for valid messages with headers -/
def pollValidWithHeaders (consumer : Consumer) (timeout_ms : Int32 := 1000) : IO (Option ConsumerMessage) := do
  match ← consumer.pollWithHeaders timeout_ms with
  | none => return none
  | some msg =>
    if msg.isOk then return some msg
    else return none

/-- Commit offsets synchronously -/
def commit (consumer : Consumer) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_commit consumer.handle 0
  KafkaResult.fromErrorCode err ()

/-- Commit offsets asynchronously -/
def commitAsync (consumer : Consumer) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_commit consumer.handle 1
  KafkaResult.fromErrorCode err ()

/-- Close the consumer -/
def close (consumer : Consumer) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_consumer_close consumer.handle
  KafkaResult.fromErrorCode err ()

/-- Get consumer group member ID -/
def memberId (consumer : Consumer) : IO (Option String) :=
  FFI.kafka_memberid consumer.handle

/-- Get consumer name -/
def name (consumer : Consumer) : IO String :=
  FFI.kafka_name consumer.handle

/-! ## Metadata -/

/-- Get cluster metadata -/
def metadata (consumer : Consumer) (allTopics : Bool := true) (timeout_ms : Int32 := 5000) : IO (Except String ClusterMetadata) := do
  match ← FFI.kafka_metadata consumer.handle (if allTopics then 1 else 0) timeout_ms with
  | .error e => return .error e
  | .ok raw => return .ok (ClusterMetadata.fromRaw raw)

/-- Get metadata for a specific topic -/
def topicMetadata (consumer : Consumer) (topic : String) (timeout_ms : Int32 := 5000) : IO (Except String TopicInfo) := do
  match ← FFI.kafka_metadata_for_topic consumer.handle topic timeout_ms with
  | .error e => return .error e
  | .ok raw => return .ok (TopicInfo.fromRaw raw)

/-- Query watermark offsets (makes broker request) -/
def queryWatermarkOffsets (consumer : Consumer) (topic : String) (partition : Int32) (timeout_ms : Int32 := 5000) : IO (Except String WatermarkOffsets) := do
  match ← FFI.kafka_query_watermark_offsets consumer.handle topic partition timeout_ms with
  | .error e => return .error e
  | .ok (lo, hi) => return .ok { low := lo, high := hi }

/-- Get cached watermark offsets -/
def getWatermarkOffsets (consumer : Consumer) (topic : String) (partition : Int32) : IO (Except String WatermarkOffsets) := do
  match ← FFI.kafka_get_watermark_offsets consumer.handle topic partition with
  | .error e => return .error e
  | .ok (lo, hi) => return .ok { low := lo, high := hi }

/-- Find offset for a timestamp -/
def offsetForTime (consumer : Consumer) (topic : String) (partition : Int32) (timestamp_ms : Int64) (timeout_ms : Int32 := 5000) : IO (Except String UInt64) :=
  FFI.kafka_offsets_for_times consumer.handle topic partition timestamp_ms timeout_ms

/-! ## Partition Assignment -/

/-- Manually assign partitions (instead of using subscribe) -/
def assign (consumer : Consumer) (partitions : PartitionList) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_assign consumer.handle partitions.ptr
  KafkaResult.fromErrorCode err ()

/-- Assign specific topic-partitions -/
def assignPartitions (consumer : Consumer) (tps : Array TopicPartition) : IO (KafkaResult Unit) := do
  match ← PartitionList.fromTopicPartitions tps with
  | none => return .error { code := .unknown, message := "Failed to create partition list" }
  | some list => consumer.assign list

/-- Assign a single topic-partition -/
def assignOne (consumer : Consumer) (topic : String) (partition : Int32) : IO (KafkaResult Unit) :=
  consumer.assignPartitions #[{ topic, partition }]

/-- Unassign all partitions -/
def unassign (consumer : Consumer) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_unassign consumer.handle
  KafkaResult.fromErrorCode err ()

/-- Get current partition assignment -/
def assignment (consumer : Consumer) : IO (Except String (Array TopicPartitionOffset)) := do
  match ← FFI.kafka_assignment consumer.handle with
  | .error e => return .error e
  | .ok ptr => do
    let list : PartitionList := { ptr }
    let arr ← list.toArray
    return .ok arr

/-- Seek to a specific offset -/
def seek (consumer : Consumer) (topic : String) (partition : Int32) (offset : Int64) (timeout_ms : Int32 := 5000) : IO (KafkaResult Unit) := do
  match ← FFI.kafka_seek consumer.handle topic partition offset timeout_ms with
  | .error e => return .error { code := .unknown, message := e }
  | .ok () => return .ok ()

/-- Seek to beginning of partition -/
def seekToBeginning (consumer : Consumer) (topic : String) (partition : Int32) (timeout_ms : Int32 := 5000) : IO (KafkaResult Unit) :=
  consumer.seek topic partition offsetBeginning timeout_ms

/-- Seek to end of partition -/
def seekToEnd (consumer : Consumer) (topic : String) (partition : Int32) (timeout_ms : Int32 := 5000) : IO (KafkaResult Unit) :=
  consumer.seek topic partition offsetEnd timeout_ms

/-- Pause consumption from partitions -/
def pause (consumer : Consumer) (partitions : PartitionList) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_pause_partitions consumer.handle partitions.ptr
  KafkaResult.fromErrorCode err ()

/-- Pause a single partition -/
def pauseOne (consumer : Consumer) (topic : String) (partition : Int32) : IO (KafkaResult Unit) := do
  match ← PartitionList.fromTopicPartitions #[{ topic, partition }] with
  | none => return .error { code := .unknown, message := "Failed to create partition list" }
  | some list => consumer.pause list

/-- Resume consumption from partitions -/
def resume (consumer : Consumer) (partitions : PartitionList) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_resume_partitions consumer.handle partitions.ptr
  KafkaResult.fromErrorCode err ()

/-- Resume a single partition -/
def resumeOne (consumer : Consumer) (topic : String) (partition : Int32) : IO (KafkaResult Unit) := do
  match ← PartitionList.fromTopicPartitions #[{ topic, partition }] with
  | none => return .error { code := .unknown, message := "Failed to create partition list" }
  | some list => consumer.resume list

/-- Store offsets for later commit -/
def storeOffsets (consumer : Consumer) (offsets : PartitionList) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_offsets_store consumer.handle offsets.ptr
  KafkaResult.fromErrorCode err ()

/-- Store offset for a message (for later commit) -/
def storeOffset (consumer : Consumer) (msg : ConsumerMessage) : IO (KafkaResult Unit) := do
  match ← PartitionList.fromTopicPartitionOffsets #[{
    topic := msg.topic,
    partition := msg.partition.toInt32,
    offset := (msg.offset + 1).toInt64  -- +1 because we want to commit the NEXT offset
  }] with
  | none => return .error { code := .unknown, message := "Failed to create partition list" }
  | some list => consumer.storeOffsets list

/-- Commit specific offsets -/
def commitOffsets (consumer : Consumer) (offsets : PartitionList) (async : Bool := false) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_commit_offsets consumer.handle offsets.ptr (if async then 1 else 0)
  KafkaResult.fromErrorCode err ()

/-- Get committed offsets for partitions -/
def committedOffsets (consumer : Consumer) (partitions : PartitionList) (timeout_ms : Int32 := 5000) : IO (KafkaResult (Array TopicPartitionOffset)) := do
  let err ← FFI.kafka_committed consumer.handle partitions.ptr timeout_ms
  if err == 0 then do
    let arr ← partitions.toArray
    return .ok arr
  else
    let kafkaErr ← KafkaError.fromCode err
    return .error kafkaErr

/-- Get current position (next offset to be read) for partitions -/
def position (consumer : Consumer) (partitions : PartitionList) : IO (KafkaResult (Array TopicPartitionOffset)) := do
  let err ← FFI.kafka_position consumer.handle partitions.ptr
  if err == 0 then do
    let arr ← partitions.toArray
    return .ok arr
  else
    let kafkaErr ← KafkaError.fromCode err
    return .error kafkaErr

/-! ## Transaction Support -/

/-- Get consumer group metadata for use in transactional offset commits -/
def groupMetadata (consumer : Consumer) : IO (Option ConsumerGroupMeta) :=
  getConsumerGroupMetadata consumer.handle

/-- Process messages with a callback -/
partial def forEach (consumer : Consumer) (timeout_ms : Int32 := 1000)
                    (f : ConsumerMessage → IO Bool) : IO Unit := do
  match ← consumer.poll timeout_ms with
  | none => consumer.forEach timeout_ms f
  | some msg =>
    if msg.isOk then
      let continue_ ← f msg
      if continue_ then
        consumer.forEach timeout_ms f
    else
      -- Skip error messages and continue
      consumer.forEach timeout_ms f

/-- Process messages for a limited number of iterations -/
def forN (consumer : Consumer) (n : Nat) (timeout_ms : Int32 := 1000)
         (f : ConsumerMessage → IO Unit) : IO Nat := do
  let mut count := 0
  for _ in [:n] do
    match ← consumer.poll timeout_ms with
    | none => pure ()
    | some msg =>
      if msg.isOk then
        f msg
        count := count + 1
  return count

/-! ## Typed Message Consumption -/

/-- Poll for a typed message using FromKafkaMessage typeclass -/
def pollTyped [FromKafkaMessage α] (consumer : Consumer) (timeout_ms : Int32 := 1000) : IO (Option α) := do
  match ← consumer.poll timeout_ms with
  | none => return none
  | some msg =>
    if msg.isOk then
      return FromKafkaMessage.fromConsumerMessage msg
    else
      return none

/-- Poll for a typed message, returning the raw message as well -/
def pollTypedWithRaw [FromKafkaMessage α] (consumer : Consumer) (timeout_ms : Int32 := 1000)
    : IO (Option (α × ConsumerMessage)) := do
  match ← consumer.poll timeout_ms with
  | none => return none
  | some msg =>
    if msg.isOk then
      match FromKafkaMessage.fromConsumerMessage msg with
      | some value => return some (value, msg)
      | none => return none
    else
      return none

/-- Process typed messages with a callback -/
partial def forEachTyped [FromKafkaMessage α] (consumer : Consumer) (timeout_ms : Int32 := 1000)
    (f : α → IO Bool) : IO Unit := do
  match ← consumer.pollTyped (α := α) timeout_ms with
  | none => consumer.forEachTyped timeout_ms f
  | some value =>
    let continue_ ← f value
    if continue_ then
      consumer.forEachTyped timeout_ms f

/-- Process typed messages with access to raw message -/
partial def forEachTypedWithRaw [FromKafkaMessage α] (consumer : Consumer) (timeout_ms : Int32 := 1000)
    (f : α → ConsumerMessage → IO Bool) : IO Unit := do
  match ← consumer.pollTypedWithRaw (α := α) timeout_ms with
  | none => consumer.forEachTypedWithRaw timeout_ms f
  | some (value, msg) =>
    let continue_ ← f value msg
    if continue_ then
      consumer.forEachTypedWithRaw timeout_ms f

/-- Process N typed messages -/
def forNTyped [FromKafkaMessage α] (consumer : Consumer) (n : Nat) (timeout_ms : Int32 := 1000)
    (f : α → IO Unit) : IO Nat := do
  let mut count := 0
  for _ in [:n] do
    match ← consumer.pollTyped (α := α) timeout_ms with
    | none => pure ()
    | some value =>
      f value
      count := count + 1
  return count

/-- Collect N typed messages into an array -/
def collectN [FromKafkaMessage α] (consumer : Consumer) (n : Nat) (timeout_ms : Int32 := 1000)
    : IO (Array α) := do
  let mut results : Array α := #[]
  for _ in [:n] do
    match ← consumer.pollTyped (α := α) timeout_ms with
    | none => pure ()
    | some value => results := results.push value
  return results

end Consumer

/-- Consumer monad transformer for convenient message consumption -/
abbrev ConsumerT (m : Type → Type) := ReaderT Consumer m

/-- Consumer monad -/
abbrev ConsumerM := ConsumerT IO

namespace ConsumerM

/-- Run a ConsumerM action with a consumer -/
def runWith (action : ConsumerM α) (consumer : Consumer) : IO α :=
  ReaderT.run action consumer

/-- Run with automatic consumer creation and cleanup -/
def withConsumer (brokers : String) (groupId : String) (topics : Array String)
                 (props : List (String × String) := [])
                 (action : ConsumerM α) : IO (Except String α) := do
  match ← Consumer.new brokers groupId props with
  | .error e => return .error e
  | .ok consumer => do
    match ← consumer.subscribe topics with
    | .error e => return .error e.message
    | .ok () => do
      let result ← action.runWith consumer
      let _ ← consumer.close
      return .ok result

/-- Poll for a message -/
def poll (timeout_ms : Int32 := 1000) : ConsumerM (Option ConsumerMessage) := do
  let consumer ← read
  consumer.poll timeout_ms

/-- Poll for a message with headers -/
def pollWithHeaders (timeout_ms : Int32 := 1000) : ConsumerM (Option ConsumerMessage) := do
  let consumer ← read
  consumer.pollWithHeaders timeout_ms

/-- Poll for valid messages only -/
def pollValid (timeout_ms : Int32 := 1000) : ConsumerM (Option ConsumerMessage) := do
  let consumer ← read
  consumer.pollValid timeout_ms

/-- Poll for valid messages with headers -/
def pollValidWithHeaders (timeout_ms : Int32 := 1000) : ConsumerM (Option ConsumerMessage) := do
  let consumer ← read
  consumer.pollValidWithHeaders timeout_ms

/-- Commit offsets -/
def commit : ConsumerM (KafkaResult Unit) := do
  let consumer ← read
  consumer.commit

/-- Commit offsets asynchronously -/
def commitAsync : ConsumerM (KafkaResult Unit) := do
  let consumer ← read
  consumer.commitAsync

/-! ## Typed Consumption in ConsumerM -/

/-- Poll for a typed message -/
def pollTyped [FromKafkaMessage α] : ConsumerM (Option α) := do
  let consumer ← read
  consumer.pollTyped (α := α)

/-- Poll for a typed message with raw message -/
def pollTypedWithRaw [FromKafkaMessage α] : ConsumerM (Option (α × ConsumerMessage)) := do
  let consumer ← read
  consumer.pollTypedWithRaw (α := α)

/-- Process N typed messages -/
def forNTyped [FromKafkaMessage α] (n : Nat) (timeout_ms : Int32 := 1000)
    (f : α → IO Unit) : ConsumerM Nat := do
  let consumer ← read
  consumer.forNTyped (α := α) n timeout_ms f

/-- Collect N typed messages -/
def collectN [FromKafkaMessage α] (n : Nat) (timeout_ms : Int32 := 1000) : ConsumerM (Array α) := do
  let consumer ← read
  consumer.collectN (α := α) n timeout_ms

end ConsumerM

end Kafka
