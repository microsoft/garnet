window.BENCHMARK_DATA = {
  "lastUpdate": 1744675408647,
  "repoUrl": "https://github.com/microsoft/garnet",
  "entries": {
    "Network.BasicOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "96085550+vazois@users.noreply.github.com",
            "name": "Vasileios Zois",
            "username": "vazois"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "14fb5f9204fa2a0483d71042b180c2df1a562c78",
          "message": "Fix Announce Regression (#1171)\n\n* explicit port declarationg at get endpoint when any is used\n\n* add unit test for IPAddres.Any cluster announce\n\n* change version",
          "timestamp": "2025-04-14T16:57:09-07:00",
          "tree_id": "b69af006462aaf52e49267dcc41210d368a6a77e",
          "url": "https://github.com/microsoft/garnet/commit/14fb5f9204fa2a0483d71042b180c2df1a562c78"
        },
        "date": 1744675200916,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 83.30432143211365,
            "unit": "ns",
            "range": "± 1.0407349984014922"
          }
        ]
      }
    ],
    "Network.BasicOperations (ubuntu-latest  net8.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "96085550+vazois@users.noreply.github.com",
            "name": "Vasileios Zois",
            "username": "vazois"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "14fb5f9204fa2a0483d71042b180c2df1a562c78",
          "message": "Fix Announce Regression (#1171)\n\n* explicit port declarationg at get endpoint when any is used\n\n* add unit test for IPAddres.Any cluster announce\n\n* change version",
          "timestamp": "2025-04-14T16:57:09-07:00",
          "tree_id": "b69af006462aaf52e49267dcc41210d368a6a77e",
          "url": "https://github.com/microsoft/garnet/commit/14fb5f9204fa2a0483d71042b180c2df1a562c78"
        },
        "date": 1744675201025,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 89.36852705478668,
            "unit": "ns",
            "range": "± 0.2999766043046506"
          }
        ]
      }
    ]
  }
}