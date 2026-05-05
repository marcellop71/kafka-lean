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

lean_lib Examples

target kafka_shim_o pkg : FilePath := do
  let srcFile := pkg.dir / "kafka" / "kafka_shim.c"
  let oFile := pkg.buildDir / "c" / "kafka_shim.o"
  IO.FS.createDirAll oFile.parent.get!
  let flags := #["-fPIC", "-O2", "-I", (← getLeanIncludeDir).toString, "-I/usr/local/include"]
  compileO oFile srcFile flags
  return .pure oFile

extern_lib libkafka_shim pkg := do
  let shimObj ← kafka_shim_o.fetch
  let name := nameToStaticLib "kafka_shim"
  buildStaticLib (pkg.staticLibDir / name) #[shimObj]

require zlogLean from git
  "git@github.com:marcellop71/zlog-lean.git" @ "v4.29.0"

lean_exe examples where
  root := `Examples.Main
  supportInterpreter := true
