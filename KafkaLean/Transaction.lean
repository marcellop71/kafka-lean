/-
  KafkaLean/Transaction.lean - Transactional producer support for exactly-once semantics
-/

import KafkaLean.FFI
import KafkaLean.Error
import KafkaLean.Config
import KafkaLean.Message
import KafkaLean.Metadata

namespace Kafka

/-! ## Transaction Error Types -/

/-- Transaction-specific error with recovery information -/
structure TxnError where
  /-- Error code -/
  code : ErrorCode
  /-- Error message -/
  message : String
  /-- If true, the producer must be recreated -/
  isFatal : Bool
  /-- If true, the operation can be retried -/
  isRetriable : Bool
  /-- If true, the transaction must be aborted before continuing -/
  txnRequiresAbort : Bool
  deriving Repr

namespace TxnError

def fromRaw (raw : FFI.TransactionError) : TxnError :=
  { code := ErrorCode.fromUInt32 raw.code
  , message := raw.message
  , isFatal := raw.isFatal
  , isRetriable := raw.isRetriable
  , txnRequiresAbort := raw.txnRequiresAbort
  }

/-- Check if this error requires recreating the producer -/
def requiresRecreate (e : TxnError) : Bool := e.isFatal

/-- Check if this error can be recovered by retrying -/
def canRetry (e : TxnError) : Bool := e.isRetriable && !e.isFatal

/-- Check if transaction needs to be aborted -/
def needsAbort (e : TxnError) : Bool := e.txnRequiresAbort

end TxnError

/-- Transaction result type -/
abbrev TxnResult (α : Type) := Except TxnError α

namespace TxnResult

def fromFFI (result : Except FFI.TransactionError Unit) : TxnResult Unit :=
  match result with
  | .ok () => .ok ()
  | .error e => .error (TxnError.fromRaw e)

end TxnResult

/-! ## Consumer Group Metadata -/

/-- Wrapper for consumer group metadata (used in send_offsets_to_transaction) -/
structure ConsumerGroupMeta where
  ptr : FFI.ConsumerGroupMetadata
  deriving Nonempty

namespace ConsumerGroupMeta

/-- Create consumer group metadata from a group ID string -/
def fromGroupId (groupId : String) : IO (Option ConsumerGroupMeta) := do
  match ← FFI.kafka_consumer_group_metadata_new groupId with
  | none => return none
  | some ptr => return some { ptr }

end ConsumerGroupMeta

/-! ## Transactional Producer -/

/-- A transactional Kafka producer with exactly-once semantics -/
structure TransactionalProducer where
  handle : FFI.Handle
  deriving Nonempty

namespace TransactionalProducer

/-- Create a new transactional producer
    Requires transactional.id and enable.idempotence=true in config -/
def create (config : Config) : IO (Except String TransactionalProducer) := do
  match ← FFI.kafka_new_producer config.ptr with
  | .error e => return .error e
  | .ok handle => return .ok { handle }

/-- Create a transactional producer with broker string and transaction ID -/
def new (brokers : String) (transactionalId : String) (props : List (String × String) := []) : IO (Except String TransactionalProducer) := do
  -- Add required transactional properties
  let txnProps := [
    ("transactional.id", transactionalId),
    ("enable.idempotence", "true")
  ] ++ props
  match ← Config.forProducer brokers txnProps with
  | .error e => return .error e
  | .ok config => create config

/-- Initialize transactions - must be called once before any transactional operations -/
def initTransactions (producer : TransactionalProducer) (timeout_ms : Int32 := 30000) : IO (TxnResult Unit) := do
  let result ← FFI.kafka_init_transactions producer.handle timeout_ms
  return TxnResult.fromFFI result

/-- Begin a new transaction -/
def beginTransaction (producer : TransactionalProducer) : IO (TxnResult Unit) := do
  let result ← FFI.kafka_begin_transaction producer.handle
  return TxnResult.fromFFI result

/-- Commit the current transaction -/
def commitTransaction (producer : TransactionalProducer) (timeout_ms : Int32 := 30000) : IO (TxnResult Unit) := do
  let result ← FFI.kafka_commit_transaction producer.handle timeout_ms
  return TxnResult.fromFFI result

/-- Abort the current transaction -/
def abortTransaction (producer : TransactionalProducer) (timeout_ms : Int32 := 30000) : IO (TxnResult Unit) := do
  let result ← FFI.kafka_abort_transaction producer.handle timeout_ms
  return TxnResult.fromFFI result

/-- Produce a message within a transaction -/
def produce (producer : TransactionalProducer) (msg : ProducerMessage) : IO (KafkaResult Unit) := do
  let err ← if msg.headers.isEmpty then
    FFI.kafka_produce producer.handle msg.topic msg.partition msg.payload msg.key
  else
    FFI.kafka_produce_with_headers producer.handle msg.topic msg.partition msg.payload msg.key msg.headers
  KafkaResult.fromErrorCode err ()

/-- Produce a string message within a transaction -/
def produceString (producer : TransactionalProducer) (topic : String) (payload : String) (key : String := "") : IO (KafkaResult Unit) :=
  producer.produce (ProducerMessage.fromString topic payload key)

/-- Poll for events (delivery reports) -/
def poll (producer : TransactionalProducer) (timeout_ms : Int32 := 0) : IO UInt32 :=
  FFI.kafka_poll producer.handle timeout_ms

/-- Flush outstanding messages -/
def flush (producer : TransactionalProducer) (timeout_ms : Int32 := 10000) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_flush producer.handle timeout_ms
  KafkaResult.fromErrorCode err ()

/-- Send consumer offsets as part of the transaction (for exactly-once consume-transform-produce) -/
def sendOffsetsToTransaction (producer : TransactionalProducer) (offsets : PartitionList) (cgMeta : ConsumerGroupMeta) (timeout_ms : Int32 := 30000) : IO (TxnResult Unit) := do
  let result ← FFI.kafka_send_offsets_to_transaction producer.handle offsets.ptr cgMeta.ptr timeout_ms
  return TxnResult.fromFFI result

/-- Get producer name -/
def name (producer : TransactionalProducer) : IO String :=
  FFI.kafka_name producer.handle

/-- Get number of messages in output queue -/
def outqLen (producer : TransactionalProducer) : IO UInt32 :=
  FFI.kafka_outq_len producer.handle

/-- Execute a transactional block - automatically begins, commits on success, aborts on failure -/
def withTransaction (producer : TransactionalProducer) (timeout_ms : Int32 := 30000) (action : IO α) : IO (TxnResult α) := do
  match ← producer.beginTransaction with
  | .error e => return .error e
  | .ok () =>
    try
      let result ← action
      let _ ← producer.poll 0
      match ← producer.commitTransaction timeout_ms with
      | .error e => return .error e
      | .ok () => return .ok result
    catch ex =>
      let _ ← producer.abortTransaction timeout_ms
      throw ex

end TransactionalProducer

/-! ## Consumer Group Metadata Helper -/

/-- Get consumer group metadata from a handle for use in transactional offset commits -/
def getConsumerGroupMetadata (handle : FFI.Handle) : IO (Option ConsumerGroupMeta) := do
  match ← FFI.kafka_consumer_group_metadata handle with
  | none => return none
  | some ptr => return some { ptr }

end Kafka
