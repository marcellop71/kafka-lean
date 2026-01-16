/-
  KafkaLean/Metadata.lean - Cluster metadata and offset queries
-/

import KafkaLean.FFI
import KafkaLean.Error

namespace Kafka

/-! ## Metadata Types -/

/-- Information about a Kafka broker -/
structure BrokerInfo where
  /-- Broker ID -/
  id : UInt32
  /-- Broker hostname -/
  host : String
  /-- Broker port -/
  port : UInt32
  deriving Repr, BEq

namespace BrokerInfo

def fromRaw (raw : FFI.RawBrokerInfo) : BrokerInfo :=
  { id := raw.id, host := raw.host, port := raw.port }

def address (b : BrokerInfo) : String := s!"{b.host}:{b.port}"

end BrokerInfo

/-- Information about a partition -/
structure PartitionInfo where
  /-- Partition ID -/
  id : UInt32
  /-- Error code if any -/
  error : ErrorCode
  /-- Leader broker ID -/
  leader : UInt32
  /-- Replica broker IDs -/
  replicas : Array UInt32
  /-- In-sync replica broker IDs -/
  isrs : Array UInt32
  deriving Repr

namespace PartitionInfo

def fromRaw (raw : FFI.RawPartitionInfo) : PartitionInfo :=
  { id := raw.id
  , error := ErrorCode.fromUInt32 raw.error
  , leader := raw.leader
  , replicas := raw.replicas
  , isrs := raw.isrs
  }

def hasError (p : PartitionInfo) : Bool := p.error.isError

def replicaCount (p : PartitionInfo) : Nat := p.replicas.size

def isrCount (p : PartitionInfo) : Nat := p.isrs.size

end PartitionInfo

/-- Information about a topic -/
structure TopicInfo where
  /-- Topic name -/
  name : String
  /-- Error code if any -/
  error : ErrorCode
  /-- Partition information -/
  partitions : Array PartitionInfo
  deriving Repr

namespace TopicInfo

def fromRaw (raw : FFI.RawTopicInfo) : TopicInfo :=
  { name := raw.name
  , error := ErrorCode.fromUInt32 raw.error
  , partitions := raw.partitions.map PartitionInfo.fromRaw
  }

def hasError (t : TopicInfo) : Bool := t.error.isError

def partitionCount (t : TopicInfo) : Nat := t.partitions.size

def getPartition (t : TopicInfo) (id : UInt32) : Option PartitionInfo :=
  t.partitions.find? fun p => p.id == id

end TopicInfo

/-- Cluster metadata -/
structure ClusterMetadata where
  /-- Broker information -/
  brokers : Array BrokerInfo
  /-- Topic information -/
  topics : Array TopicInfo
  /-- Broker ID of the responding broker -/
  origBrokerId : UInt32
  /-- Name of the responding broker -/
  origBrokerName : String
  deriving Repr

namespace ClusterMetadata

def fromRaw (raw : FFI.RawClusterMetadata) : ClusterMetadata :=
  { brokers := raw.brokers.map BrokerInfo.fromRaw
  , topics := raw.topics.map TopicInfo.fromRaw
  , origBrokerId := raw.origBrokerId
  , origBrokerName := raw.origBrokerName
  }

def brokerCount (m : ClusterMetadata) : Nat := m.brokers.size

def topicCount (m : ClusterMetadata) : Nat := m.topics.size

def topicNames (m : ClusterMetadata) : Array String :=
  m.topics.map (·.name)

def getTopic (m : ClusterMetadata) (name : String) : Option TopicInfo :=
  m.topics.find? fun t => t.name == name

def getBroker (m : ClusterMetadata) (id : UInt32) : Option BrokerInfo :=
  m.brokers.find? fun b => b.id == id

end ClusterMetadata

/-! ## Watermark Offsets -/

/-- Watermark offsets (low and high) for a partition -/
structure WatermarkOffsets where
  /-- Lowest available offset -/
  low : UInt64
  /-- Highest available offset (next to be written) -/
  high : UInt64
  deriving Repr, BEq

namespace WatermarkOffsets

def messageCount (w : WatermarkOffsets) : UInt64 := w.high - w.low

end WatermarkOffsets

/-! ## Topic Partition -/

/-- A topic-partition identifier -/
structure TopicPartition where
  topic : String
  partition : Int32
  deriving Repr, BEq, Hashable

/-- A topic-partition with offset -/
structure TopicPartitionOffset where
  topic : String
  partition : Int32
  offset : Int64
  deriving Repr, BEq

/-- Special offset values -/
def offsetBeginning : Int64 := -2
def offsetEnd : Int64 := -1
def offsetStored : Int64 := -1000
def offsetInvalid : Int64 := -1001

/-! ## Topic Partition List -/

/-- Wrapper around FFI TopicPartitionList for partition assignments -/
structure PartitionList where
  ptr : FFI.TopicPartitionList
  deriving Nonempty

namespace PartitionList

/-- Create a new partition list -/
def new (initialSize : UInt32 := 8) : IO (Option PartitionList) := do
  match ← FFI.kafka_topic_partition_list_new initialSize with
  | none => return none
  | some ptr => return some { ptr }

/-- Add a topic-partition to the list -/
def add (list : PartitionList) (topic : String) (partition : Int32) : IO Unit :=
  FFI.kafka_topic_partition_list_add list.ptr topic partition

/-- Add a range of partitions for a topic -/
def addRange (list : PartitionList) (topic : String) (start stop : Int32) : IO Unit :=
  FFI.kafka_topic_partition_list_add_range list.ptr topic start stop

/-- Set offset for a specific topic-partition -/
def setOffset (list : PartitionList) (topic : String) (partition : Int32) (offset : Int64) : IO (KafkaResult Unit) := do
  let err ← FFI.kafka_topic_partition_list_set_offset list.ptr topic partition offset
  KafkaResult.fromErrorCode err ()

/-- Convert to array of topic-partition-offset tuples -/
def toArray (list : PartitionList) : IO (Array TopicPartitionOffset) := do
  let raw ← FFI.kafka_topic_partition_list_to_array list.ptr
  return raw.map fun (topic, partition, offset, _) =>
    { topic, partition := partition.toInt32, offset := offset.toInt64 }

/-- Create from array of TopicPartition -/
def fromTopicPartitions (tps : Array TopicPartition) : IO (Option PartitionList) := do
  match ← new tps.size.toUInt32 with
  | none => return none
  | some list => do
    for tp in tps do
      list.add tp.topic tp.partition
    return some list

/-- Create from array of TopicPartitionOffset -/
def fromTopicPartitionOffsets (tpos : Array TopicPartitionOffset) : IO (Option PartitionList) := do
  match ← new tpos.size.toUInt32 with
  | none => return none
  | some list => do
    for tpo in tpos do
      list.add tpo.topic tpo.partition
      let _ ← list.setOffset tpo.topic tpo.partition tpo.offset
    return some list

end PartitionList

end Kafka
