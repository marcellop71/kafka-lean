/-
  KafkaLean - Lean 4 bindings for Apache Kafka via librdkafka

  This module provides type-safe access to Kafka producers and consumers
  with both low-level FFI bindings and high-level monadic interfaces.

  ## Features

  - **Low-level FFI**: Direct bindings to librdkafka
  - **High-level API**: Producer and Consumer with ergonomic methods
  - **Type-safe messaging**: ToKafkaMessage/FromKafkaMessage typeclasses
  - **JSON support**: ToKafkaJson/FromKafkaJson for automatic serialization
  - **Topic patterns**: Type-safe topic naming conventions
  - **Batch production**: Efficient bulk message production
  - **Monadic interfaces**: ProducerM and ConsumerM for convenient use

  ## Quick Start

  ```lean
  import KafkaLean

  -- Define your message type
  structure TradeMsg where
    symbol : String
    price : Float
    volume : Nat
    deriving ToJson, FromJson

  -- Implement the typeclass for type-safe production
  instance : ToKafkaJson TradeMsg where
    topic := fun msg => s!"trades:{msg.symbol}"
    key := fun msg => some msg.symbol

  -- Enable JSON deserialization
  instance : FromKafkaJson TradeMsg := {}

  -- Produce messages
  def publishTrade (producer : Kafka.Producer) (trade : TradeMsg) : IO Unit := do
    let _ ← producer.produceTyped trade

  -- Consume messages
  def consumeTrades (consumer : Kafka.Consumer) : IO Unit := do
    consumer.forEachTyped (α := TradeMsg) fun trade => do
      IO.println s!"Got trade: {trade.symbol} @ {trade.price}"
      return true  -- continue
  ```
-/

import KafkaLean.FFI
import KafkaLean.Error
import KafkaLean.Config
import KafkaLean.Message
import KafkaLean.Topic
import KafkaLean.Codec
import KafkaLean.Typeclass
import KafkaLean.Metadata
import KafkaLean.Transaction
import KafkaLean.Producer
import KafkaLean.Consumer

namespace Kafka

/-- Get librdkafka version string -/
def version : IO String := FFI.kafka_version

/-- Get error description for error code -/
def errorString (code : UInt32) : IO String := FFI.kafka_err2str code.toInt32

/-! ## Re-exports for convenience -/

-- Topic patterns
export TopicPattern (create build forSymbol bulk stockTrades stockQuotes optionTrades optionQuotes indexTrades)
export TopicBuilder (new withPrefix withSecurityType withDataType stock option index trades quotes ohlc build forSymbol bulk)

-- Codec
export Codec (encode decode enc dec decodeOption)

-- Routing
export KafkaRouting (topic key)

-- Type classes
export ToKafkaMessage (topic key serialize toProducerMessage)
export ToKafkaJson (topic key)
export FromKafkaMessage (deserialize deserializeExcept fromConsumerMessage fromConsumerMessageExcept)

-- Message headers
export MessageHeaders (empty fromStrings addString add get getString getAll contains)

-- Metadata
export BrokerInfo (id host port address fromRaw)
export PartitionInfo (id error leader replicas isrs hasError replicaCount isrCount fromRaw)
export TopicInfo (name error partitions hasError partitionCount getPartition fromRaw)
export ClusterMetadata (brokers topics origBrokerId origBrokerName brokerCount topicCount topicNames getTopic getBroker fromRaw)
export WatermarkOffsets (low high messageCount)
export TopicPartition (topic partition)
export TopicPartitionOffset (topic partition offset)

-- Transactions
export TxnError (code message isFatal isRetriable txnRequiresAbort requiresRecreate canRetry needsAbort fromRaw)
export TransactionalProducer (create new initTransactions beginTransaction commitTransaction abortTransaction produce produceString poll flush sendOffsetsToTransaction name outqLen withTransaction)
export ConsumerGroupMeta (fromGroupId)

end Kafka
