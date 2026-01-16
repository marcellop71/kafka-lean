/-
  Examples/Main.lean - Kafka-Lean usage examples
-/

import KafkaLean
import ZlogLean

open Kafka

/-- Example: Produce messages to a topic -/
def producerExample (brokers : String) (topic : String) : IO Unit := do
  Zlog.info "Starting producer example"

  match ← Producer.new brokers with
  | .error e => Zlog.error s!"Failed to create producer: {e}"
  | .ok producer => do
    Zlog.info s!"Created producer: {← producer.name}"

    -- Produce some messages
    for i in [:10] do
      let payload := s!"Message {i} from Lean!"
      let key := s!"key-{i}"
      match ← producer.produceString topic payload key with
      | .error e => Zlog.error s!"Failed to produce: {e.message}"
      | .ok () => Zlog.debug s!"Produced message {i}"

      -- Poll for callbacks
      let _ ← producer.poll 0

    -- Flush remaining messages
    Zlog.info "Flushing messages..."
    match ← producer.flush 10000 with
    | .error e => Zlog.error s!"Flush error: {e.message}"
    | .ok () => Zlog.info "All messages delivered"

/-- Example: Consume messages from a topic -/
def consumerExample (brokers : String) (groupId : String) (topic : String) (maxMessages : Nat := 10) : IO Unit := do
  Zlog.info "Starting consumer example"

  match ← Consumer.new brokers groupId with
  | .error e => Zlog.error s!"Failed to create consumer: {e}"
  | .ok consumer => do
    Zlog.info s!"Created consumer: {← consumer.name}"

    -- Subscribe to topic
    match ← consumer.subscribeTo topic with
    | .error e => Zlog.error s!"Failed to subscribe: {e.message}"
    | .ok () => do
      Zlog.info s!"Subscribed to {topic}"

      -- Consume messages
      let count ← consumer.forN maxMessages 1000 fun msg => do
        match msg.payloadString with
        | some payload =>
          Zlog.info s!"Received: {payload} (partition={msg.partition}, offset={msg.offset})"
        | none =>
          Zlog.debug s!"Received binary message ({msg.payload.size} bytes)"

      Zlog.info s!"Consumed {count} messages"

      -- Commit and close
      let _ ← consumer.commit
      let _ ← consumer.close
      Zlog.info "Consumer closed"

/-- Example: Using ProducerM monad -/
def producerMonadExample (brokers : String) (topic : String) : IO Unit := do
  Zlog.info "Starting ProducerM example"

  match ← ProducerM.withProducer brokers [] (do
    for i in [:5] do
      match ← ProducerM.produceString topic s!"Monadic message {i}" with
      | .error e => Zlog.error s!"Error: {e.message}"
      | .ok () => Zlog.debug s!"Sent message {i}"
      let _ ← ProducerM.poll 0
    ProducerM.flush 5000
  ) with
  | .error e => Zlog.error s!"Producer error: {e}"
  | .ok result =>
    match result with
    | .error e => Zlog.error s!"Flush error: {e.message}"
    | .ok () => Zlog.info "ProducerM example completed"

/-- Example: Using ConsumerM monad -/
def consumerMonadExample (brokers : String) (groupId : String) (topic : String) : IO Unit := do
  Zlog.info "Starting ConsumerM example"

  match ← ConsumerM.withConsumer brokers groupId #[topic] [] (do
    let mut count := 0
    for _ in [:10] do
      match ← ConsumerM.pollValid 1000 with
      | none => pure ()
      | some msg =>
        match msg.payloadString with
        | some s => Zlog.info s!"[ConsumerM] {s}"
        | none => pure ()
        count := count + 1
    let _ ← ConsumerM.commit
    return count
  ) with
  | .error e => Zlog.error s!"Consumer error: {e}"
  | .ok count => Zlog.info s!"ConsumerM processed {count} messages"

def main : IO UInt32 := do
  -- Print version
  let ver ← Kafka.version
  Zlog.info s!"librdkafka version: {ver}"

  -- Configuration (prefixed with _ to suppress unused warnings when examples are commented out)
  let _brokers := "localhost:9092"
  let _topic := "test-topic"
  let _groupId := "lean-consumer-group"

  Zlog.info "Kafka-Lean Examples"
  Zlog.info "==================="
  Zlog.info ""
  Zlog.info "To run these examples, ensure Kafka is running on localhost:9092"
  Zlog.info "and the topic 'test-topic' exists."
  Zlog.info ""
  Zlog.info "Uncomment the example calls below to run them:"
  Zlog.info ""
  Zlog.info "  -- producerExample brokers topic"
  Zlog.info "  -- consumerExample brokers groupId topic"
  Zlog.info "  -- producerMonadExample brokers topic"
  Zlog.info "  -- consumerMonadExample brokers groupId topic"

  -- Uncomment to run examples:
  -- producerExample _brokers _topic
  -- consumerExample _brokers _groupId _topic
  -- producerMonadExample _brokers _topic
  -- consumerMonadExample _brokers _groupId _topic

  return 0
