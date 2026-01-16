/-
  KafkaLean/Codec.lean - Bidirectional encoding/decoding for Kafka messages

  The Codec class provides a clean separation between serialization logic
  and message routing. This allows the same encoding to be reused across
  different messaging systems (Kafka, Redis, etc.)
-/

import Lean.Data.Json

namespace Kafka

/-- Codec α means that the type α can be encoded to and decoded from a ByteArray.
    This is the core serialization abstraction, independent of any messaging system. -/
class Codec (α : Type u) where
  /-- Encode a value to bytes -/
  encode : α → ByteArray
  /-- Decode bytes to a value, with error message on failure -/
  decode : ByteArray → Except String α

namespace Codec

/-- Encode a value using its Codec instance -/
def enc [Codec α] (a : α) : ByteArray := encode a

/-- Decode bytes using its Codec instance -/
def dec [Codec α] (bytes : ByteArray) : Except String α := decode bytes

/-- Decode bytes, returning Option instead of Except -/
def decodeOption [Codec α] (bytes : ByteArray) : Option α :=
  match decode bytes with
  | .ok a => some a
  | .error _ => none

end Codec

/-! ## Primitive Codec instances -/

instance : Codec ByteArray where
  encode := id
  decode := .ok

instance : Codec Unit where
  encode _ := ByteArray.empty
  decode _ := .ok ()

instance : Codec String where
  encode := String.toUTF8
  decode bytes :=
    match String.fromUTF8? bytes with
    | some str => .ok str
    | none => .error "Invalid UTF-8 bytes"

instance : Codec Nat where
  encode n := String.toUTF8 (toString n)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str.toNat? with
    | some n => .ok n
    | none => .error s!"Cannot decode '{str}' as Nat"

instance : Codec Int where
  encode n := String.toUTF8 (toString n)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str.toInt? with
    | some n => .ok n
    | none => .error s!"Cannot decode '{str}' as Int"

instance : Codec Int64 where
  encode n := String.toUTF8 (toString n)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str.toInt? with
    | some n => .ok n.toInt64
    | none => .error s!"Cannot decode '{str}' as Int64"

instance : Codec Int32 where
  encode n := String.toUTF8 (toString n)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str.toInt? with
    | some n => .ok n.toInt32
    | none => .error s!"Cannot decode '{str}' as Int32"

instance : Codec UInt64 where
  encode n := String.toUTF8 (toString n)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str.toNat? with
    | some n => .ok n.toUInt64
    | none => .error s!"Cannot decode '{str}' as UInt64"

instance : Codec UInt32 where
  encode n := String.toUTF8 (toString n)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str.toNat? with
    | some n => .ok n.toUInt32
    | none => .error s!"Cannot decode '{str}' as UInt32"

instance : Codec Bool where
  encode b := String.toUTF8 (if b then "true" else "false")
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    match str with
    | "true" => .ok true
    | "false" => .ok false
    | "1" => .ok true
    | "0" => .ok false
    | _ => .error s!"Cannot decode '{str}' as Bool"

instance : Codec Float where
  encode f := String.toUTF8 (toString f)
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    -- Note: Lean's Float doesn't have a direct fromString, using a workaround
    match Lean.Json.parse str with
    | .ok (Lean.Json.num n) => .ok n.toFloat
    | _ => .error s!"Cannot decode '{str}' as Float"

/-! ## JSON Codec -/

/-- JSON codec for any type that has ToJson and FromJson instances.
    This is a lower-priority instance to avoid conflicts with primitive types. -/
instance (priority := low) [Lean.ToJson α] [Lean.FromJson α] : Codec α where
  encode a := String.toUTF8 (Lean.Json.compress (Lean.toJson a))
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    let json ← Lean.Json.parse str |>.mapError (s!"JSON parse error: {·}")
    Lean.fromJson? json |>.mapError (s!"JSON decode error: {·}")

/-! ## Codec combinators -/

/-- Codec for Option types - uses empty ByteArray for none -/
instance [Codec α] : Codec (Option α) where
  encode
    | none => ByteArray.empty
    | some a => Codec.encode a
  decode bytes :=
    if bytes.size == 0 then
      .ok none
    else
      match Codec.decode bytes with
      | .ok a => .ok (some a)
      | .error e => .error e

/-- Codec for Array using JSON serialization -/
instance [Lean.ToJson α] [Lean.FromJson α] : Codec (Array α) where
  encode arr := String.toUTF8 (Lean.Json.compress (Lean.toJson arr))
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    let json ← Lean.Json.parse str |>.mapError (s!"JSON parse error: {·}")
    Lean.fromJson? json |>.mapError (s!"JSON decode error: {·}")

/-- Codec for List using JSON serialization -/
instance [Lean.ToJson α] [Lean.FromJson α] : Codec (List α) where
  encode lst := String.toUTF8 (Lean.Json.compress (Lean.toJson lst))
  decode bytes := do
    let str ← match String.fromUTF8? bytes with
      | some s => .ok s
      | none => .error "Invalid UTF-8 bytes"
    let json ← Lean.Json.parse str |>.mapError (s!"JSON parse error: {·}")
    Lean.fromJson? json |>.mapError (s!"JSON decode error: {·}")

end Kafka
