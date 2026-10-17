import Lake
open System Lake DSL

package kafkaLean where
  extraDepTargets := #[`libkafka_shim]
  moreLinkArgs := #[
    "-L/usr/local/lib",
    "-Wl,-rpath,/usr/local/lib",
    "-Wl,--allow-shlib-undefined",
    "-lzlog",
    "-L/usr/lib/x86_64-linux-gnu",
    "-Wl,-rpath,/usr/lib/x86_64-linux-gnu",
    "-lrdkafka"
  ]

@[default_target]
lean_lib KafkaLean

lean_lib Examples where
  globs := #[.submodules `Examples]

-- Traced build: the object (and the archive below) is rebuilt when kafka_shim.c
-- changes. A `compileO` + `.pure` job has no trace, so edits to the shim never
-- reached libkafka_shim.a.
target kafka_shim_o pkg : FilePath := do
  let oFile := pkg.buildDir / "c" / "kafka_shim.o"
  let srcJob ← inputTextFile <| pkg.dir / "kafka" / "kafka_shim.c"
  -- Path-dependent, kept out of the trace
  let weakArgs := #["-I", (← getLeanIncludeDir).toString, "-I/usr/local/include"]
  buildO oFile srcJob weakArgs #["-fPIC", "-O2"]

extern_lib libkafka_shim pkg := do
  let shimObj ← kafka_shim_o.fetch
  let name := nameToStaticLib "kafka_shim"
  buildStaticLib (pkg.staticLibDir / name) #[shimObj]

require zlogLean from git
  "git@github.com:marcellop71/zlog-lean.git" @ "v4.35.0"

lean_exe examples where
  root := `Examples.Main
  supportInterpreter := true
