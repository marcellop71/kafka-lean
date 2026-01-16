/-
  KafkaLean/FFI.lean - Low-level FFI bindings to librdkafka
-/

namespace Kafka.FFI

/-- Opaque type for Kafka configuration pointer -/
opaque ConfPointer : NonemptyType
def Conf := ConfPointer.type
instance : Nonempty Conf := ConfPointer.property

/-- Opaque type for Kafka topic configuration pointer -/
opaque TopicConfPointer : NonemptyType
def TopicConf := TopicConfPointer.type
instance : Nonempty TopicConf := TopicConfPointer.property

/-- Opaque type for Kafka handle (producer/consumer) -/
opaque HandlePointer : NonemptyType
def Handle := HandlePointer.type
instance : Nonempty Handle := HandlePointer.property

/-- Opaque type for Kafka topic handle -/
opaque TopicPointer : NonemptyType
def Topic := TopicPointer.type
instance : Nonempty Topic := TopicPointer.property

/-- Opaque type for Kafka headers -/
opaque HeadersPointer : NonemptyType
def Headers := HeadersPointer.type
instance : Nonempty Headers := HeadersPointer.property

/-- Opaque type for topic partition list -/
opaque TopicPartitionListPointer : NonemptyType
def TopicPartitionList := TopicPartitionListPointer.type
instance : Nonempty TopicPartitionList := TopicPartitionListPointer.property

/-- Opaque type for consumer group metadata -/
opaque ConsumerGroupMetadataPointer : NonemptyType
def ConsumerGroupMetadata := ConsumerGroupMetadataPointer.type
instance : Nonempty ConsumerGroupMetadata := ConsumerGroupMetadataPointer.property

/-! ## Version and Error Functions -/

@[extern "lean_kafka_version"]
opaque kafka_version : IO String

@[extern "lean_kafka_err2str"]
opaque kafka_err2str (err : Int32) : IO String

@[extern "lean_kafka_last_error"]
opaque kafka_last_error : IO UInt32

/-! ## Configuration -/

@[extern "lean_kafka_conf_new"]
opaque kafka_conf_new : IO (Option Conf)

@[extern "lean_kafka_conf_dup"]
opaque kafka_conf_dup (conf : @& Conf) : IO (Option Conf)

@[extern "lean_kafka_conf_set"]
opaque kafka_conf_set (conf : @& Conf) (name : @& String) (value : @& String) : IO (Except String Unit)

@[extern "lean_kafka_conf_get"]
opaque kafka_conf_get (conf : @& Conf) (name : @& String) : IO (Option String)

/-! ## Topic Configuration -/

@[extern "lean_kafka_topic_conf_new"]
opaque kafka_topic_conf_new : IO (Option TopicConf)

@[extern "lean_kafka_topic_conf_set"]
opaque kafka_topic_conf_set (conf : @& TopicConf) (name : @& String) (value : @& String) : IO (Except String Unit)

/-! ## Producer -/

@[extern "lean_kafka_new_producer"]
opaque kafka_new_producer (conf : @& Conf) : IO (Except String Handle)

@[extern "lean_kafka_produce"]
opaque kafka_produce (handle : @& Handle) (topic : @& String) (partition : Int32)
                     (payload : @& ByteArray) (key : @& ByteArray) : IO UInt32

@[extern "lean_kafka_flush"]
opaque kafka_flush (handle : @& Handle) (timeout_ms : Int32) : IO UInt32

@[extern "lean_kafka_poll"]
opaque kafka_poll (handle : @& Handle) (timeout_ms : Int32) : IO UInt32

@[extern "lean_kafka_outq_len"]
opaque kafka_outq_len (handle : @& Handle) : IO UInt32

@[extern "lean_kafka_produce_with_headers"]
opaque kafka_produce_with_headers (handle : @& Handle) (topic : @& String) (partition : Int32)
                                   (payload : @& ByteArray) (key : @& ByteArray)
                                   (headers : @& Array (String × ByteArray)) : IO UInt32

/-! ## Consumer -/

@[extern "lean_kafka_new_consumer"]
opaque kafka_new_consumer (conf : @& Conf) : IO (Except String Handle)

@[extern "lean_kafka_subscribe"]
opaque kafka_subscribe (handle : @& Handle) (topics : @& Array String) : IO UInt32

@[extern "lean_kafka_unsubscribe"]
opaque kafka_unsubscribe (handle : @& Handle) : IO UInt32

/-- Raw message structure returned from C -/
structure RawMessage where
  topic : String
  partition : UInt32
  offset : UInt64
  key : ByteArray
  payload : ByteArray
  error : UInt32

/-- Raw message structure with headers returned from C -/
structure RawMessageWithHeaders where
  topic : String
  partition : UInt32
  offset : UInt64
  key : ByteArray
  payload : ByteArray
  error : UInt32
  headers : Array (String × ByteArray)

@[extern "lean_kafka_consumer_poll"]
opaque kafka_consumer_poll (handle : @& Handle) (timeout_ms : Int32) : IO (Option RawMessage)

@[extern "lean_kafka_consumer_poll_with_headers"]
opaque kafka_consumer_poll_with_headers (handle : @& Handle) (timeout_ms : Int32) : IO (Option RawMessageWithHeaders)

@[extern "lean_kafka_consumer_close"]
opaque kafka_consumer_close (handle : @& Handle) : IO UInt32

/-! ## Commit -/

@[extern "lean_kafka_commit"]
opaque kafka_commit (handle : @& Handle) (async : UInt8) : IO UInt32

/-! ## Metadata -/

@[extern "lean_kafka_memberid"]
opaque kafka_memberid (handle : @& Handle) : IO (Option String)

@[extern "lean_kafka_name"]
opaque kafka_name (handle : @& Handle) : IO String

/-! ## Cleanup -/

@[extern "lean_kafka_destroy"]
opaque kafka_destroy (handle : Handle) : IO Unit

/-! ## Headers -/

@[extern "lean_kafka_headers_new"]
opaque kafka_headers_new : IO (Option Headers)

@[extern "lean_kafka_header_add"]
opaque kafka_header_add (headers : @& Headers) (name : @& String) (value : @& ByteArray) : IO UInt32

@[extern "lean_kafka_headers_count"]
opaque kafka_headers_count (headers : @& Headers) : IO UInt64

@[extern "lean_kafka_headers_to_array"]
opaque kafka_headers_to_array (headers : @& Headers) : IO (Array (String × ByteArray))

/-! ## Metadata -/

/-- Raw broker info returned from C -/
structure RawBrokerInfo where
  id : UInt32
  host : String
  port : UInt32

/-- Raw partition info returned from C -/
structure RawPartitionInfo where
  id : UInt32
  error : UInt32
  leader : UInt32
  replicas : Array UInt32
  isrs : Array UInt32

/-- Raw topic info returned from C -/
structure RawTopicInfo where
  name : String
  error : UInt32
  partitions : Array RawPartitionInfo

/-- Raw cluster metadata returned from C -/
structure RawClusterMetadata where
  brokers : Array RawBrokerInfo
  topics : Array RawTopicInfo
  origBrokerId : UInt32
  origBrokerName : String

@[extern "lean_kafka_metadata"]
opaque kafka_metadata (handle : @& Handle) (allTopics : UInt8) (timeout_ms : Int32) : IO (Except String RawClusterMetadata)

@[extern "lean_kafka_metadata_for_topic"]
opaque kafka_metadata_for_topic (handle : @& Handle) (topic : @& String) (timeout_ms : Int32) : IO (Except String RawTopicInfo)

@[extern "lean_kafka_query_watermark_offsets"]
opaque kafka_query_watermark_offsets (handle : @& Handle) (topic : @& String) (partition : Int32) (timeout_ms : Int32) : IO (Except String (UInt64 × UInt64))

@[extern "lean_kafka_get_watermark_offsets"]
opaque kafka_get_watermark_offsets (handle : @& Handle) (topic : @& String) (partition : Int32) : IO (Except String (UInt64 × UInt64))

@[extern "lean_kafka_offsets_for_times"]
opaque kafka_offsets_for_times (handle : @& Handle) (topic : @& String) (partition : Int32) (timestamp_ms : Int64) (timeout_ms : Int32) : IO (Except String UInt64)

/-! ## Topic Partition List -/

@[extern "lean_kafka_topic_partition_list_new"]
opaque kafka_topic_partition_list_new (size : UInt32) : IO (Option TopicPartitionList)

@[extern "lean_kafka_topic_partition_list_add"]
opaque kafka_topic_partition_list_add (list : @& TopicPartitionList) (topic : @& String) (partition : Int32) : IO Unit

@[extern "lean_kafka_topic_partition_list_add_range"]
opaque kafka_topic_partition_list_add_range (list : @& TopicPartitionList) (topic : @& String) (start : Int32) (stop : Int32) : IO Unit

@[extern "lean_kafka_topic_partition_list_set_offset"]
opaque kafka_topic_partition_list_set_offset (list : @& TopicPartitionList) (topic : @& String) (partition : Int32) (offset : Int64) : IO UInt32

@[extern "lean_kafka_topic_partition_list_to_array"]
opaque kafka_topic_partition_list_to_array (list : @& TopicPartitionList) : IO (Array (String × UInt32 × UInt64 × UInt32))

/-! ## Partition Assignment -/

@[extern "lean_kafka_assign"]
opaque kafka_assign (handle : @& Handle) (partitions : @& TopicPartitionList) : IO UInt32

@[extern "lean_kafka_unassign"]
opaque kafka_unassign (handle : @& Handle) : IO UInt32

@[extern "lean_kafka_assignment"]
opaque kafka_assignment (handle : @& Handle) : IO (Except String TopicPartitionList)

@[extern "lean_kafka_seek"]
opaque kafka_seek (handle : @& Handle) (topic : @& String) (partition : Int32) (offset : Int64) (timeout_ms : Int32) : IO (Except String Unit)

@[extern "lean_kafka_pause_partitions"]
opaque kafka_pause_partitions (handle : @& Handle) (partitions : @& TopicPartitionList) : IO UInt32

@[extern "lean_kafka_resume_partitions"]
opaque kafka_resume_partitions (handle : @& Handle) (partitions : @& TopicPartitionList) : IO UInt32

@[extern "lean_kafka_offsets_store"]
opaque kafka_offsets_store (handle : @& Handle) (offsets : @& TopicPartitionList) : IO UInt32

@[extern "lean_kafka_commit_offsets"]
opaque kafka_commit_offsets (handle : @& Handle) (offsets : @& TopicPartitionList) (async : UInt8) : IO UInt32

@[extern "lean_kafka_committed"]
opaque kafka_committed (handle : @& Handle) (partitions : @& TopicPartitionList) (timeout_ms : Int32) : IO UInt32

@[extern "lean_kafka_position"]
opaque kafka_position (handle : @& Handle) (partitions : @& TopicPartitionList) : IO UInt32

/-! ## Transactions -/

/-- Transaction error with detailed status -/
structure TransactionError where
  code : UInt32
  message : String
  isFatal : Bool
  isRetriable : Bool
  txnRequiresAbort : Bool

@[extern "lean_kafka_init_transactions"]
opaque kafka_init_transactions (handle : @& Handle) (timeout_ms : Int32) : IO (Except TransactionError Unit)

@[extern "lean_kafka_begin_transaction"]
opaque kafka_begin_transaction (handle : @& Handle) : IO (Except TransactionError Unit)

@[extern "lean_kafka_commit_transaction"]
opaque kafka_commit_transaction (handle : @& Handle) (timeout_ms : Int32) : IO (Except TransactionError Unit)

@[extern "lean_kafka_abort_transaction"]
opaque kafka_abort_transaction (handle : @& Handle) (timeout_ms : Int32) : IO (Except TransactionError Unit)

@[extern "lean_kafka_consumer_group_metadata"]
opaque kafka_consumer_group_metadata (handle : @& Handle) : IO (Option ConsumerGroupMetadata)

@[extern "lean_kafka_consumer_group_metadata_new"]
opaque kafka_consumer_group_metadata_new (groupId : @& String) : IO (Option ConsumerGroupMetadata)

@[extern "lean_kafka_send_offsets_to_transaction"]
opaque kafka_send_offsets_to_transaction (handle : @& Handle) (offsets : @& TopicPartitionList) (cgmetadata : @& ConsumerGroupMetadata) (timeout_ms : Int32) : IO (Except TransactionError Unit)

end Kafka.FFI
