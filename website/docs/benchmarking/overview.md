---
id: overview
sidebar_label: Overview
title: Overview of Benchmarking
---

## Client Tool

We have evaluated Garnet using a benchmarking client tool we wrote, called Resp.benchmark. The tool is written to carefully control
various parameters, and to avoid client side overheads so that the focus can be on the server. Learn more about the tool [here](resp-bench).

## BDN Performance metrics

We have incorporated BDN ([BenchmarkDotNet](https://benchmarkdotnet.org/)) to track and monitor performance as part of the Garnet CI (Garnet CI BDN.benchmark). The BDN CI runs on each push to main and logs the metrics from that run for both Windows and Linux. 

The BDN CI first looks at the "Allocated" bytes from that specific run and compares it to what is expected. If that found "Allocated" value is 10% more than the expected "Allocated" value, the BDN CI will fail. If that part of the CI passes, then the "Mean" value is compared to the "Mean" value of the previous run. At this point, the CI also logs the "Mean" value so it can be charted [here](https://microsoft.github.io/garnet/charts/).

**To add your BDN to Garnet BDN CI**
1) The GitHub action yml: \.github\workflows\ci-bdnbenchmark.yml

    a) Update the "test" with the addition of your BDN test (~line 45)
    
    b) Do not add BDN.benchmark prefix in your test in this yml file
    
2) Add the expected results to \test\BDNPerfTests\BDN_Benchmark_Config.json

    a) These values are the expected "Allocated" bytes. If the found value is more than 10% it will show the GH action as a Fail.
    
    b) Follow the pattern for the labels of expected values that others have and use "_".
    
    c) If the expected Allocated value for Linux and Windows are different, use the value that is higher.

    d) If the expected values are not highly deterministic, it is advisable to avoid failing the test when these values are out of compliance. To prevent false failures, prepend WARN-ON-FAIL_ instead of expected_ to each expected value. This ensures the values are checked and will only generate warnings without causing a pipeline failure. The values will still be included in the charts, even if they are marked as warnings.
 
**To run BDN perf tests locally**
test/BDNPerfTests/run_bdnperftest.ps1 

1) Only parameter (optional) is BDN test. For example, Operations.BasicOperations (default), Operations.ObjectOperations, Cluster.ClusterMigrate, Lua.LuaScripts etc
     
2) It will verify found Allocated vs expected Allocated (giving it 10% tolerance) and show results in the output.
     
3) BDN test results are in test/BDNPerfTests/results/*.results with the summary at end of file.
