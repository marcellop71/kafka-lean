/-
  KafkaLean/Typeclass.lean - Type classes for typed Kafka message production and consumption

  This module provides a layered approach to Kafka message handling:
  1. Codec - Core serialization (encode/decode ByteArray)
  2. KafkaRouting - Topic and partition key routing
  3. ToKafkaMessage/FromKafkaMessage - Combined interfaces for Kafka producers/consumers
  4. JSON variants - Convenient shortcuts for JSON-serialized messages
-/

import Lean.Data.Json
import KafkaLean.Message
import KafkaLean.Topic
import KafkaLean.Codec

namespace Kafka

open Lean

/-! ## KafkaRouting - Topic and key routing -/

/-- Typeclass for determining Kafka routing (topic and partition key).
    This is separate from serialization to allow flexible composition. -/
class KafkaRouting (α : Type) where
  /-- Get the topic name for this message -/
  topic : α → String
  /-- Get the partition key (used for consistent hashing). None means round-robin. -/
  key : α → Option String := fun _ => none

/-! ## ToKafkaMessage - Core typeclass for producing typed messages -/

/-- Typeclass for types that can be serialized to Kafka messages.
    Implement this to enable type-safe message production.

    You can either:
    1. Implement ToKafkaMessage directly
    2. Implement Codec + KafkaRouting (ToKafkaMessage will be derived)
    3. Implement ToKafkaJson for JSON messages (easiest) -/
class ToKafkaMessage (α : Type) where
  /-- Get the topic name for this message -/
  topic : α → String
  /-- Get the partition key (used for consistent hashing). None means round-robin. -/
  key : α → Option String := fun _ => none
  /-- Serialize the message to bytes -/
  serialize : α → ByteArray

namespace ToKafkaMessage

/-- Convert a typed message to a ProducerMessage -/
def toProducerMessage [ToKafkaMessage α] (msg : α) : ProducerMessage :=
  { topic := topic msg
  , payload := serialize msg
  , key := match key msg with
    | some k => k.toUTF8
    | none => ByteArray.empty
  }

end ToKafkaMessage

/-- Derive ToKafkaMessage from Codec + KafkaRouting -/
instance (priority := low) [Codec α] [KafkaRouting α] : ToKafkaMessage α where
  topic := KafkaRouting.topic
  key := KafkaRouting.key
  serialize := Codec.encode

/-! ## ToKafkaJson - JSON-specific variant -/

/-- Typeclass for types that serialize to Kafka as JSON.
    Automatically derives serialization from ToJson.
    This is the easiest way to produce typed Kafka messages. -/
class ToKafkaJson (α : Type) [ToJson α] where
  /-- Get the topic name for this message -/
  topic : α → String
  /-- Get the partition key (used for consistent hashing). None means round-robin. -/
  key : α → Option String := fun _ => none

/-- Automatically derive ToKafkaMessage from ToKafkaJson + ToJson -/
instance [ToJson α] [ToKafkaJson α] : ToKafkaMessage α where
  topic := ToKafkaJson.topic
  key := ToKafkaJson.key
  serialize := fun a => (Json.compress (toJson a)).toUTF8

/-! ## ToKafkaJsonWithPattern - JSON with TopicPattern -/

/-- Typeclass for types that use TopicPattern for topic naming -/
class ToKafkaJsonWithPattern (α : Type) [ToJson α] where
  /-- Get the topic pattern for this message type -/
  pattern : TopicPattern
  /-- Extract the symbol from the message (for topic suffix) -/
  symbol : α → String
  /-- Get the partition key. Defaults to symbol. -/
  key : α → Option String := fun a => some (symbol a)

/-- Automatically derive ToKafkaJson from ToKafkaJsonWithPattern -/
instance [ToJson α] [ToKafkaJsonWithPattern α] : ToKafkaJson α where
  topic := fun a => (ToKafkaJsonWithPattern.pattern (α := α)).forSymbol (ToKafkaJsonWithPattern.symbol a)
  key := ToKafkaJsonWithPattern.key

/-! ## FromKafkaMessage - Core typeclass for consuming typed messages -/

/-- Typeclass for types that can be deserialized from Kafka messages.
    Implement this to enable type-safe message consumption.

    You can either:
    1. Implement FromKafkaMessage directly
    2. Implement Codec (FromKafkaMessage will be derived)
    3. Implement FromKafkaJson for JSON messages (easiest) -/
class FromKafkaMessage (α : Type) where
  /-- Deserialize bytes to a typed message. Returns none on parse failure. -/
  deserialize : ByteArray → Option α
  /-- Deserialize with error message. Returns Except for detailed errors. -/
  deserializeExcept : ByteArray → Except String α := fun bytes =>
    match deserialize bytes with
    | some a => .ok a
    | none => .error "Deserialization failed"

namespace FromKafkaMessage

/-- Try to deserialize a ConsumerMessage to a typed value -/
def fromConsumerMessage [FromKafkaMessage α] (msg : ConsumerMessage) : Option α :=
  deserialize msg.payload

/-- Try to deserialize a ConsumerMessage with error details -/
def fromConsumerMessageExcept [FromKafkaMessage α] (msg : ConsumerMessage) : Except String α :=
  deserializeExcept msg.payload

end FromKafkaMessage

/-- Derive FromKafkaMessage from Codec -/
instance (priority := low) [Codec α] : FromKafkaMessage α where
  deserialize := Codec.decodeOption
  deserializeExcept := Codec.decode

/-! ## FromKafkaJson - JSON-specific variant -/

/-- Typeclass for types that deserialize from Kafka as JSON.
    Automatically derives deserialization from FromJson. -/
class FromKafkaJson (α : Type) [FromJson α]

/-- Automatically derive FromKafkaMessage from FromKafkaJson + FromJson -/
instance [FromJson α] [FromKafkaJson α] : FromKafkaMessage α where
  deserialize := fun bytes =>
    match String.fromUTF8? bytes with
    | none => none
    | some str =>
      match Json.parse str with
      | .error _ => none
      | .ok json =>
        match FromJson.fromJson? json with
        | .error _ => none
        | .ok value => some value
  deserializeExcept := fun bytes => do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    let json ← Json.parse str |>.mapError (s!"JSON parse error: {·}")
    FromJson.fromJson? json |>.mapError (s!"JSON decode error: {·}")

/-! ## Convenience instances -/

/-- String messages serialize as UTF-8 -/
instance : KafkaRouting String where
  topic := fun _ => "default"

/-- ByteArray messages pass through -/
instance : KafkaRouting ByteArray where
  topic := fun _ => "default"

/-! ## Message wrapper for explicit topic/key control -/

/-- Wrapper that allows overriding topic and key for any serializable type -/
structure KafkaMsg (α : Type) where
  /-- The message payload -/
  payload : α
  /-- Topic to publish to -/
  topic : String
  /-- Optional partition key -/
  key : Option String := none

namespace KafkaMsg

/-- Create a KafkaMsg with just topic -/
def create (payload : α) (topic : String) : KafkaMsg α :=
  { payload, topic }

/-- Create a KafkaMsg with topic and key -/
def createWithKey (payload : α) (topic : String) (key : String) : KafkaMsg α :=
  { payload, topic, key := some key }

end KafkaMsg

/-- KafkaRouting instance for wrapped messages -/
instance : KafkaRouting (KafkaMsg α) where
  topic := fun m => m.topic
  key := fun m => m.key

/-- Codec instance for KafkaMsg delegates to payload's Codec -/
instance [Codec α] : Codec (KafkaMsg α) where
  encode := fun m => Codec.encode m.payload
  decode := fun bytes => do
    let payload ← Codec.decode bytes
    -- Note: We can't recover topic/key from bytes, use defaults
    .ok { payload, topic := "unknown", key := none }

/-- ToKafkaMessage instance for wrapped messages with JSON -/
instance [ToJson α] : ToKafkaMessage (KafkaMsg α) where
  topic := fun m => m.topic
  key := fun m => m.key
  serialize := fun m => (Json.compress (toJson m.payload)).toUTF8

/-! ## Bulk message support -/

/-- Configuration for bulk topic publishing -/
structure BulkTopicConfig where
  /-- The bulk topic name -/
  topic : String
  /-- Function to extract key from message -/
  keyExtractor : String → Option String := fun _ => none

/-- Wrapper for publishing to bulk topics -/
structure BulkMsg (α : Type) where
  payload : α
  config : BulkTopicConfig
  /-- Raw key value (extracted from payload) -/
  rawKey : String := ""

instance : KafkaRouting (BulkMsg α) where
  topic := fun m => m.config.topic
  key := fun m => m.config.keyExtractor m.rawKey

instance [Codec α] : Codec (BulkMsg α) where
  encode := fun m => Codec.encode m.payload
  decode := fun bytes => do
    let payload ← Codec.decode bytes
    .ok { payload, config := { topic := "unknown" }, rawKey := "" }

instance [ToJson α] : ToKafkaMessage (BulkMsg α) where
  topic := fun m => m.config.topic
  key := fun m => m.config.keyExtractor m.rawKey
  serialize := fun m => (Json.compress (toJson m.payload)).toUTF8

end Kafka
