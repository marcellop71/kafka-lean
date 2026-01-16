/-
  KafkaLean/Topic.lean - Type-safe topic patterns and naming conventions
-/

namespace Kafka

/-! ## Security Types -/

/-- Common security types for financial data -/
inductive SecurityType where
  | stock
  | option
  | index
  | future
  | forex
  | crypto
  | other (name : String)
  deriving Repr, BEq, Inhabited

instance : ToString SecurityType where
  toString
    | .stock => "stock"
    | .option => "option"
    | .index => "index"
    | .future => "future"
    | .forex => "forex"
    | .crypto => "crypto"
    | .other name => name

namespace SecurityType

def fromString (s : String) : SecurityType :=
  match s.toLower with
  | "stock" => .stock
  | "option" => .option
  | "index" => .index
  | "future" => .future
  | "forex" => .forex
  | "crypto" => .crypto
  | s => .other s

end SecurityType

/-! ## Data Types -/

/-- Common data types for market data -/
inductive DataType where
  | trades
  | quotes
  | ohlc
  | depth
  | stats
  | news
  | other (name : String)
  deriving Repr, BEq, Inhabited

instance : ToString DataType where
  toString
    | .trades => "trades"
    | .quotes => "quotes"
    | .ohlc => "ohlc"
    | .depth => "depth"
    | .stats => "stats"
    | .news => "news"
    | .other name => name

namespace DataType

def fromString (s : String) : DataType :=
  match s.toLower with
  | "trades" => .trades
  | "quotes" => .quotes
  | "ohlc" => .ohlc
  | "depth" => .depth
  | "stats" => .stats
  | "news" => .news
  | s => .other s

end DataType

/-! ## Topic Patterns -/

/-- A pattern for building topic names consistently -/
structure TopicPattern where
  /-- Prefix for all topics (e.g., "thetadata", "market") -/
  topicPrefix : String := "data"
  /-- Security type component -/
  securityType : SecurityType := .stock
  /-- Data type component -/
  dataType : DataType := .trades
  /-- Separator between components -/
  separator : String := ":"
  deriving Repr, Inhabited

namespace TopicPattern

/-- Create a topic pattern with common defaults -/
def create (pfx : String) (secType : SecurityType) (dt : DataType) : TopicPattern :=
  { topicPrefix := pfx, securityType := secType, dataType := dt }

/-- Build a topic name without a symbol suffix -/
def build (p : TopicPattern) : String :=
  s!"{p.topicPrefix}{p.separator}{p.securityType}{p.separator}{p.dataType}"

/-- Build a topic name with a symbol suffix -/
def forSymbol (p : TopicPattern) (symbol : String) : String :=
  s!"{p.build}{p.separator}{symbol}"

/-- Build a bulk topic name (no symbol) -/
def bulk (p : TopicPattern) : String :=
  s!"{p.build}{p.separator}bulk"

/-- Common patterns -/
def stockTrades (pfx : String := "data") : TopicPattern :=
  { topicPrefix := pfx, securityType := .stock, dataType := .trades }

def stockQuotes (pfx : String := "data") : TopicPattern :=
  { topicPrefix := pfx, securityType := .stock, dataType := .quotes }

def optionTrades (pfx : String := "data") : TopicPattern :=
  { topicPrefix := pfx, securityType := .option, dataType := .trades }

def optionQuotes (pfx : String := "data") : TopicPattern :=
  { topicPrefix := pfx, securityType := .option, dataType := .quotes }

def indexTrades (pfx : String := "data") : TopicPattern :=
  { topicPrefix := pfx, securityType := .index, dataType := .trades }

end TopicPattern

/-! ## Topic Builder -/

/-- Fluent builder for topic patterns -/
structure TopicBuilder where
  pattern : TopicPattern := {}
  deriving Repr

namespace TopicBuilder

def new : TopicBuilder := {}

def withPrefix (b : TopicBuilder) (pfx : String) : TopicBuilder :=
  { b with pattern := { b.pattern with topicPrefix := pfx } }

def withSecurityType (b : TopicBuilder) (st : SecurityType) : TopicBuilder :=
  { b with pattern := { b.pattern with securityType := st } }

def withDataType (b : TopicBuilder) (dt : DataType) : TopicBuilder :=
  { b with pattern := { b.pattern with dataType := dt } }

def withSeparator (b : TopicBuilder) (sep : String) : TopicBuilder :=
  { b with pattern := { b.pattern with separator := sep } }

def stock (b : TopicBuilder) : TopicBuilder := b.withSecurityType .stock
def option (b : TopicBuilder) : TopicBuilder := b.withSecurityType .option
def index (b : TopicBuilder) : TopicBuilder := b.withSecurityType .index

def trades (b : TopicBuilder) : TopicBuilder := b.withDataType .trades
def quotes (b : TopicBuilder) : TopicBuilder := b.withDataType .quotes
def ohlc (b : TopicBuilder) : TopicBuilder := b.withDataType .ohlc

def build (b : TopicBuilder) : String := b.pattern.build
def forSymbol (b : TopicBuilder) (symbol : String) : String := b.pattern.forSymbol symbol
def bulk (b : TopicBuilder) : String := b.pattern.bulk

end TopicBuilder

/-- Convenience function to start building a topic -/
def topic : TopicBuilder := TopicBuilder.new

end Kafka
