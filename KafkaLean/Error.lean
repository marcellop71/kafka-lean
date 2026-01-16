/-
  KafkaLean/Error.lean - Kafka error handling
-/

import KafkaLean.FFI

namespace Kafka

/-- Kafka response error codes -/
inductive ErrorCode
  | noError
  | unknown
  | brokerNotAvailable
  | invalidMessage
  | unknownTopicOrPartition
  | invalidPartition
  | leaderNotAvailable
  | notLeaderForPartition
  | requestTimedOut
  | messageSizeTooLarge
  | topicAuthorizationFailed
  | groupAuthorizationFailed
  | clusterAuthorizationFailed
  | invalidTimestamp
  | unsupportedVersion
  | topicAlreadyExists
  | invalidPartitions
  | invalidReplicationFactor
  | invalidConfig
  | notController
  | invalidRequest
  | msgSizeTooLarge
  | unknownMemberId
  | rebalanceInProgress
  | commitFailed
  | allBrokersDown
  | other (code : UInt32)
  deriving Repr, BEq

namespace ErrorCode

def fromUInt32 (code : UInt32) : ErrorCode :=
  match code with
  | 0 => .noError
  | 1 => .unknown
  | 8 => .brokerNotAvailable
  | 2 => .invalidMessage
  | 3 => .unknownTopicOrPartition
  | 4 => .invalidPartition
  | 5 => .leaderNotAvailable
  | 6 => .notLeaderForPartition
  | 7 => .requestTimedOut
  | 10 => .messageSizeTooLarge
  | 29 => .topicAuthorizationFailed
  | 30 => .groupAuthorizationFailed
  | 31 => .clusterAuthorizationFailed
  | 32 => .invalidTimestamp
  | 35 => .unsupportedVersion
  | 36 => .topicAlreadyExists
  | 37 => .invalidPartitions
  | 38 => .invalidReplicationFactor
  | 40 => .invalidConfig
  | 41 => .notController
  | 42 => .invalidRequest
  | n => .other n

def toUInt32 : ErrorCode → UInt32
  | .noError => 0
  | .unknown => 1
  | .brokerNotAvailable => 8
  | .invalidMessage => 2
  | .unknownTopicOrPartition => 3
  | .invalidPartition => 4
  | .leaderNotAvailable => 5
  | .notLeaderForPartition => 6
  | .requestTimedOut => 7
  | .messageSizeTooLarge => 10
  | .topicAuthorizationFailed => 29
  | .groupAuthorizationFailed => 30
  | .clusterAuthorizationFailed => 31
  | .invalidTimestamp => 32
  | .unsupportedVersion => 35
  | .topicAlreadyExists => 36
  | .invalidPartitions => 37
  | .invalidReplicationFactor => 38
  | .invalidConfig => 40
  | .notController => 41
  | .invalidRequest => 42
  | .msgSizeTooLarge => 10
  | .unknownMemberId => 25
  | .rebalanceInProgress => 27
  | .commitFailed => 28
  | .allBrokersDown => 13
  | .other n => n

def isError (e : ErrorCode) : Bool :=
  e != .noError

end ErrorCode

/-- Kafka error with message -/
structure KafkaError where
  code : ErrorCode
  message : String
  deriving Repr

namespace KafkaError

def fromCode (code : UInt32) : IO KafkaError := do
  let errorCode := ErrorCode.fromUInt32 code
  let message ← FFI.kafka_err2str code.toInt32
  return { code := errorCode, message }

def noError : KafkaError :=
  { code := .noError, message := "No error" }

def isOk (e : KafkaError) : Bool :=
  e.code == .noError

end KafkaError

/-- Result type for Kafka operations -/
abbrev KafkaResult (α : Type) := Except KafkaError α

namespace KafkaResult

def fromErrorCode (code : UInt32) (value : α) : IO (KafkaResult α) := do
  if code == 0 then
    return .ok value
  else
    let err ← KafkaError.fromCode code
    return .error err

end KafkaResult

end Kafka
