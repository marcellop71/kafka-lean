/-
  KafkaLean/Config.lean - Kafka configuration management
-/

import KafkaLean.FFI
import KafkaLean.Error

namespace Kafka

/-- Kafka configuration wrapper -/
structure Config where
  ptr : FFI.Conf
  deriving Nonempty

namespace Config

/-- Create a new configuration -/
def new : IO (Option Config) := do
  let conf ← FFI.kafka_conf_new
  return conf.map (Config.mk ·)

/-- Duplicate a configuration -/
def dup (config : Config) : IO (Option Config) := do
  let conf ← FFI.kafka_conf_dup config.ptr
  return conf.map (Config.mk ·)

/-- Set a configuration property -/
def set (config : Config) (name : String) (value : String) : IO (Except String Unit) :=
  FFI.kafka_conf_set config.ptr name value

/-- Get a configuration property -/
def get (config : Config) (name : String) : IO (Option String) :=
  FFI.kafka_conf_get config.ptr name

/-- Set multiple configuration properties -/
def setMany (config : Config) (props : List (String × String)) : IO (Except String Unit) := do
  for (name, value) in props do
    match ← config.set name value with
    | .error e => return .error e
    | .ok () => pure ()
  return .ok ()

/-- Common producer configuration -/
def forProducer (brokers : String) (props : List (String × String) := []) : IO (Except String Config) := do
  let some config ← Config.new | return .error "Failed to create configuration"
  match ← config.set "bootstrap.servers" brokers with
  | .error e => return .error e
  | .ok () => pure ()
  match ← config.setMany props with
  | .error e => return .error e
  | .ok () => return .ok config

/-- Common consumer configuration -/
def forConsumer (brokers : String) (groupId : String) (props : List (String × String) := []) : IO (Except String Config) := do
  let some config ← Config.new | return .error "Failed to create configuration"
  match ← config.set "bootstrap.servers" brokers with
  | .error e => return .error e
  | .ok () => pure ()
  match ← config.set "group.id" groupId with
  | .error e => return .error e
  | .ok () => pure ()
  -- Set sensible defaults for consumer
  match ← config.set "auto.offset.reset" "earliest" with
  | .error _ => pure ()  -- Ignore if not supported
  | .ok () => pure ()
  match ← config.setMany props with
  | .error e => return .error e
  | .ok () => return .ok config

end Config

/-- Topic configuration wrapper -/
structure TopicConfig where
  ptr : FFI.TopicConf
  deriving Nonempty

namespace TopicConfig

/-- Create a new topic configuration -/
def new : IO (Option TopicConfig) := do
  let conf ← FFI.kafka_topic_conf_new
  return conf.map (TopicConfig.mk ·)

/-- Set a topic configuration property -/
def set (config : TopicConfig) (name : String) (value : String) : IO (Except String Unit) :=
  FFI.kafka_topic_conf_set config.ptr name value

end TopicConfig

end Kafka
