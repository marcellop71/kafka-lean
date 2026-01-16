/-
  KafkaLean/Producer.lean - High-level Kafka producer API
-/

import KafkaLean.FFI
import KafkaLean.Error
import KafkaLean.Config
import KafkaLean.Message
import KafkaLean.Typeclass
import KafkaLean.Metadata

namespace Kafka

/-- Kafka producer -/
structure Producer where
  handle : FFI.Handle
  deriving Nonempty

namespace Producer

/-- Create a new producer -/
def create (config : Config) : IO (Except String Producer) := do
  match ← FFI.kafka_new_producer config.ptr with
  | .error e => return .error e
  | .ok handle => return .ok { handle }

/-- Create a producer with broker string -/
def new (brokers : String) (props : List (String × String) := []) : IO (Except String Producer) := do
  match ← Config.forProducer brokers props with
  | .error e => return .error e
  | .ok config => create config

/-- Produce a message -/
def produce (producer : Producer) (msg : ProducerMessage) : IO (KafkaResult Unit) := do
  let err ← if msg.headers.isEmpty then
    FFI.kafka_produce producer.handle msg.topic msg.partition msg.payload msg.key
  else
    FFI.kafka_produce_with_headers producer.handle msg.topic msg.partition msg.payload msg.key msg.headers
  KafkaResult.fromErrorCode err ()

/-- Produce a string message -/
def produceString (producer : Producer) (topic : String) (payload : String) (key : String := "") (headers : MessageHeaders := #[]) : IO (KafkaResult Unit) :=
  producer.produce (ProducerMessage.fromString topic payload key headers)

/-- Produce a ByteArray message -/
def produceBytes (producer : Producer) (topic : String) (payload : ByteArray) (key : ByteArray := ByteArray.empty) (headers : MessageHeaders := #[]) : IO (KafkaResult Unit) :=
  producer.produce { topic, payload, key, headers }

/-- Produce a message with headers -/
def produceWithHeaders (producer : Producer) (topic : String) (payload : ByteArray) (key : ByteArray := ByteArray.empty) (headers : MessageHeaders) : IO (KafkaResult Unit) :=
  producer.produce { topic, payload, key, headers }

/-- Poll for events (callbacks) -/
def poll (producer : Producer) (timeout_ms : Int32 := 0) : IO UInt32 :=
  FFI.kafka_poll producer.handle timeout_ms

/-- Flush outstanding messages -/
def flush (producer : Producer) (timeout_ms : Int32 := 10000) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_flush producer.handle timeout_ms
  KafkaResult.fromErrorCode err ()

/-- Get number of messages in output queue -/
def outqLen (producer : Producer) : IO UInt32 :=
  FFI.kafka_outq_len producer.handle

/-- Get producer name -/
def name (producer : Producer) : IO String :=
  FFI.kafka_name producer.handle

/-! ## Metadata -/

/-- Get cluster metadata -/
def metadata (producer : Producer) (allTopics : Bool := true) (timeout_ms : Int32 := 5000) : IO (Except String ClusterMetadata) := do
  match ← FFI.kafka_metadata producer.handle (if allTopics then 1 else 0) timeout_ms with
  | .error e => return .error e
  | .ok raw => return .ok (ClusterMetadata.fromRaw raw)

/-- Get metadata for a specific topic -/
def topicMetadata (producer : Producer) (topic : String) (timeout_ms : Int32 := 5000) : IO (Except String TopicInfo) := do
  match ← FFI.kafka_metadata_for_topic producer.handle topic timeout_ms with
  | .error e => return .error e
  | .ok raw => return .ok (TopicInfo.fromRaw raw)

/-- Wait for all messages to be delivered -/
def waitForDelivery (producer : Producer) (timeout_ms : Int32 := 30000) : IO (KafkaResult Unit) := do
  let mut remaining := timeout_ms
  while remaining > 0 do
    let _ ← producer.poll 100
    let qlen ← producer.outqLen
    if qlen == 0 then
      return .ok ()
    remaining := remaining - 100
  producer.flush timeout_ms

/-! ## Typed Message Production -/

/-- Produce a typed message using ToKafkaMessage typeclass -/
def produceTyped [ToKafkaMessage α] (producer : Producer) (msg : α) : IO (KafkaResult Unit) :=
  producer.produce (ToKafkaMessage.toProducerMessage msg)

/-- Produce a typed message and poll for delivery reports -/
def produceTypedWithPoll [ToKafkaMessage α] (producer : Producer) (msg : α) : IO (KafkaResult Unit) := do
  let result ← producer.produceTyped msg
  let _ ← producer.poll 0
  return result

/-- Produce a JSON message to a specific topic -/
def produceJson [Lean.ToJson α] (producer : Producer) (topic : String) (msg : α) (key : Option String := none) : IO (KafkaResult Unit) :=
  let kafkaMsg : KafkaMsg α := { payload := msg, topic, key }
  producer.produceTyped kafkaMsg

end Producer

/-! ## Batch Production -/

/-- Result of batch production -/
structure BatchResult where
  /-- Number of messages successfully queued -/
  succeeded : Nat
  /-- Number of messages that failed -/
  failed : Nat
  /-- First error encountered (if any) -/
  firstError : Option KafkaError
  deriving Repr

namespace BatchResult

def isOk (r : BatchResult) : Bool := r.failed == 0

def total (r : BatchResult) : Nat := r.succeeded + r.failed

end BatchResult

namespace Producer

/-- Produce a batch of typed messages -/
def produceBatch [ToKafkaMessage α] (producer : Producer) (msgs : Array α) : IO BatchResult := do
  let mut succeeded := 0
  let mut failed := 0
  let mut firstError : Option KafkaError := none
  for msg in msgs do
    match ← producer.produceTyped msg with
    | .ok () => succeeded := succeeded + 1
    | .error e =>
      failed := failed + 1
      if firstError.isNone then
        firstError := some e
  -- Poll after batch
  let _ ← producer.poll 0
  return { succeeded, failed, firstError }

/-- Produce a batch with periodic polling (for large batches) -/
def produceBatchWithPolling [ToKafkaMessage α] (producer : Producer) (msgs : Array α)
    (pollEvery : Nat := 100) : IO BatchResult := do
  let mut succeeded := 0
  let mut failed := 0
  let mut firstError : Option KafkaError := none
  let mut count := 0
  for msg in msgs do
    match ← producer.produceTyped msg with
    | .ok () => succeeded := succeeded + 1
    | .error e =>
      failed := failed + 1
      if firstError.isNone then
        firstError := some e
    count := count + 1
    if count % pollEvery == 0 then
      let _ ← producer.poll 0
  -- Final poll
  let _ ← producer.poll 0
  return { succeeded, failed, firstError }

/-- Produce a batch of JSON messages to a single topic -/
def produceBatchJson [Lean.ToJson α] (producer : Producer) (topic : String) (msgs : Array α)
    (keyFn : α → Option String := fun _ => none) : IO BatchResult := do
  let kafkaMsgs := msgs.map fun msg => KafkaMsg.create msg topic |> fun m => { m with key := keyFn msg }
  producer.produceBatch kafkaMsgs

end Producer

/-- Producer monad transformer for convenient message production -/
abbrev ProducerT (m : Type → Type) := ReaderT Producer m

/-- Producer monad -/
abbrev ProducerM := ProducerT IO

namespace ProducerM

/-- Run a ProducerM action with a producer -/
def runWith (action : ProducerM α) (producer : Producer) : IO α :=
  ReaderT.run action producer

/-- Run with automatic producer creation and cleanup -/
def withProducer (brokers : String) (props : List (String × String) := [])
                 (action : ProducerM α) : IO (Except String α) := do
  match ← Producer.new brokers props with
  | .error e => return .error e
  | .ok producer => do
    let result ← action.runWith producer
    let _ ← producer.flush 10000
    return .ok result

/-- Produce a message in ProducerM context -/
def produce (msg : ProducerMessage) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.produce msg

/-- Produce a string message in ProducerM context -/
def produceString (topic : String) (payload : String) (key : String := "") (headers : MessageHeaders := #[]) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.produceString topic payload key headers

/-- Produce a message with headers in ProducerM context -/
def produceWithHeaders (topic : String) (payload : ByteArray) (key : ByteArray := ByteArray.empty) (headers : MessageHeaders) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.produceWithHeaders topic payload key headers

/-- Poll for events -/
def poll (timeout_ms : Int32 := 0) : ProducerM UInt32 := do
  let producer ← read
  producer.poll timeout_ms

/-- Flush messages -/
def flush (timeout_ms : Int32 := 10000) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.flush timeout_ms

/-! ## Typed Production in ProducerM -/

/-- Produce a typed message -/
def produceTyped [ToKafkaMessage α] (msg : α) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.produceTyped msg

/-- Produce a typed message with polling -/
def produceTypedWithPoll [ToKafkaMessage α] (msg : α) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.produceTypedWithPoll msg

/-- Produce a JSON message to a specific topic -/
def produceJson [Lean.ToJson α] (topic : String) (msg : α) (key : Option String := none) : ProducerM (KafkaResult Unit) := do
  let producer ← read
  producer.produceJson topic msg key

/-- Produce a batch of typed messages -/
def produceBatch [ToKafkaMessage α] (msgs : Array α) : ProducerM BatchResult := do
  let producer ← read
  liftM $ producer.produceBatch msgs

/-- Produce a batch with periodic polling -/
def produceBatchWithPolling [ToKafkaMessage α] (msgs : Array α) (pollEvery : Nat := 100) : ProducerM BatchResult := do
  let producer ← read
  liftM $ producer.produceBatchWithPolling msgs pollEvery

/-- Produce a batch of JSON messages to a single topic -/
def produceBatchJson [Lean.ToJson α] (topic : String) (msgs : Array α)
    (keyFn : α → Option String := fun _ => none) : ProducerM BatchResult := do
  let producer ← read
  liftM $ producer.produceBatchJson topic msgs keyFn

end ProducerM

end Kafka
