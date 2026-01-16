/-
  KafkaLean/Message.lean - Kafka message types
-/

import KafkaLean.FFI
import KafkaLean.Error

namespace Kafka

/-- Special partition value for automatic partitioning -/
def partitionUnassigned : Int32 := -1

/-- Message headers type - array of key-value pairs -/
abbrev MessageHeaders := Array (String × ByteArray)

namespace MessageHeaders

/-- Empty headers -/
def empty : MessageHeaders := #[]

/-- Create headers from list of string key-value pairs -/
def fromStrings (pairs : List (String × String)) : MessageHeaders :=
  pairs.toArray.map fun (k, v) => (k, v.toUTF8)

/-- Add a header with string value -/
def addString (hdrs : MessageHeaders) (name : String) (value : String) : MessageHeaders :=
  hdrs.push (name, value.toUTF8)

/-- Add a header with ByteArray value -/
def add (hdrs : MessageHeaders) (name : String) (value : ByteArray) : MessageHeaders :=
  hdrs.push (name, value)

/-- Get header value by name (first match) -/
def get (hdrs : MessageHeaders) (name : String) : Option ByteArray :=
  hdrs.find? (fun (k, _) => k == name) |>.map (·.2)

/-- Get header value as string by name (first match) -/
def getString (hdrs : MessageHeaders) (name : String) : Option String :=
  hdrs.get name >>= String.fromUTF8?

/-- Get all values for a header name -/
def getAll (hdrs : MessageHeaders) (name : String) : Array ByteArray :=
  hdrs.filter (fun (k, _) => k == name) |>.map (·.2)

/-- Check if headers contain a key -/
def contains (hdrs : MessageHeaders) (name : String) : Bool :=
  hdrs.any fun (k, _) => k == name

end MessageHeaders

/-- Kafka message for producing -/
structure ProducerMessage where
  topic : String
  partition : Int32 := partitionUnassigned
  key : ByteArray := ByteArray.empty
  payload : ByteArray
  headers : MessageHeaders := #[]

namespace ProducerMessage

/-- Create a message with string payload -/
def fromString (topic : String) (payload : String) (key : String := "") (headers : MessageHeaders := #[]) : ProducerMessage :=
  { topic
  , payload := payload.toUTF8
  , key := if key.isEmpty then ByteArray.empty else key.toUTF8
  , headers
  }

/-- Create a message with string payload and specific partition -/
def fromStringWithPartition (topic : String) (partition : Int32) (payload : String) (key : String := "") (headers : MessageHeaders := #[]) : ProducerMessage :=
  { topic
  , partition
  , payload := payload.toUTF8
  , key := if key.isEmpty then ByteArray.empty else key.toUTF8
  , headers
  }

/-- Add headers to an existing message -/
def withHeaders (msg : ProducerMessage) (headers : MessageHeaders) : ProducerMessage :=
  { msg with headers }

/-- Add a single header to a message -/
def addHeader (msg : ProducerMessage) (name : String) (value : ByteArray) : ProducerMessage :=
  { msg with headers := msg.headers.push (name, value) }

/-- Add a single header with string value -/
def addHeaderString (msg : ProducerMessage) (name : String) (value : String) : ProducerMessage :=
  { msg with headers := msg.headers.push (name, value.toUTF8) }

end ProducerMessage

/-- Kafka message received from consumer -/
structure ConsumerMessage where
  topic : String
  partition : UInt32
  offset : UInt64
  key : ByteArray
  payload : ByteArray
  error : ErrorCode
  headers : MessageHeaders := #[]

namespace ConsumerMessage

/-- Create from raw FFI message -/
def fromRaw (raw : FFI.RawMessage) : ConsumerMessage :=
  { topic := raw.topic
  , partition := raw.partition
  , offset := raw.offset
  , key := raw.key
  , payload := raw.payload
  , error := ErrorCode.fromUInt32 raw.error
  , headers := #[]
  }

/-- Create from raw FFI message with headers -/
def fromRawWithHeaders (raw : FFI.RawMessageWithHeaders) : ConsumerMessage :=
  { topic := raw.topic
  , partition := raw.partition
  , offset := raw.offset
  , key := raw.key
  , payload := raw.payload
  , error := ErrorCode.fromUInt32 raw.error
  , headers := raw.headers
  }

/-- Get payload as string (UTF-8) -/
def payloadString (msg : ConsumerMessage) : Option String :=
  String.fromUTF8? msg.payload

/-- Get key as string (UTF-8) -/
def keyString (msg : ConsumerMessage) : Option String :=
  if msg.key.isEmpty then none
  else String.fromUTF8? msg.key

/-- Check if message has an error -/
def hasError (msg : ConsumerMessage) : Bool :=
  msg.error.isError

/-- Check if message is valid (no error) -/
def isOk (msg : ConsumerMessage) : Bool :=
  !msg.hasError

/-- Get header value by name -/
def getHeader (msg : ConsumerMessage) (name : String) : Option ByteArray :=
  msg.headers.get name

/-- Get header value as string by name -/
def getHeaderString (msg : ConsumerMessage) (name : String) : Option String :=
  msg.headers.getString name

/-- Check if message has headers -/
def hasHeaders (msg : ConsumerMessage) : Bool :=
  !msg.headers.isEmpty

end ConsumerMessage

end Kafka
