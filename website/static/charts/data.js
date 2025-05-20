window.BENCHMARK_DATA = {
  "lastUpdate": 1747777182613,
  "repoUrl": "https://github.com/microsoft/garnet",
  "entries": {
    "Lua.LuaRunnerOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775597364,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3299.84375,
            "unit": "ns",
            "range": "± 411.22623631549885"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3343.7894736842104,
            "unit": "ns",
            "range": "± 440.84958825259906"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 422371,
            "unit": "ns",
            "range": "± 12528.774483921623"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 432887.3888888889,
            "unit": "ns",
            "range": "± 9144.1242972413"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 18362.020833333332,
            "unit": "ns",
            "range": "± 2990.588620037575"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 150294.80412371134,
            "unit": "ns",
            "range": "± 19661.90408698547"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3468.866666666667,
            "unit": "ns",
            "range": "± 67.24248940180888"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3630.5,
            "unit": "ns",
            "range": "± 77.6668980893632"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 385882.9747474748,
            "unit": "ns",
            "range": "± 46271.79415886614"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 474602.3333333333,
            "unit": "ns",
            "range": "± 19842.06277126619"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 15129.916666666666,
            "unit": "ns",
            "range": "± 206.3009533020469"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 158667.09595959596,
            "unit": "ns",
            "range": "± 27286.455635747498"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3632.5208333333335,
            "unit": "ns",
            "range": "± 319.8316655652558"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 3259.5,
            "unit": "ns",
            "range": "± 38.14093298846751"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 399611.26,
            "unit": "ns",
            "range": "± 63205.432160682096"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 345763.46875,
            "unit": "ns",
            "range": "± 10518.322966673442"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 15425.925531914894,
            "unit": "ns",
            "range": "± 1203.7180593635233"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 137680.85882352942,
            "unit": "ns",
            "range": "± 7525.938286943335"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3634.4666666666667,
            "unit": "ns",
            "range": "± 63.558596885828415"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3858.935483870968,
            "unit": "ns",
            "range": "± 238.73492697475956"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 447712.4130434783,
            "unit": "ns",
            "range": "± 16915.00307823546"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 458350.3214285714,
            "unit": "ns",
            "range": "± 13023.152252682943"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 20079.225806451614,
            "unit": "ns",
            "range": "± 1591.3783547674764"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 156423.1224489796,
            "unit": "ns",
            "range": "± 22397.914317602572"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3248.9473684210525,
            "unit": "ns",
            "range": "± 75.95354843894943"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3344.5714285714284,
            "unit": "ns",
            "range": "± 47.33954971303558"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 440597.35185185185,
            "unit": "ns",
            "range": "± 12121.624785429816"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 444316.39130434784,
            "unit": "ns",
            "range": "± 11016.72873298987"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 21666.1,
            "unit": "ns",
            "range": "± 394.2372019845051"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 132781.07692307694,
            "unit": "ns",
            "range": "± 1325.3918578756536"
          }
        ]
      }
    ],
    "Network.BasicOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775603000,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 84.14275300502777,
            "unit": "ns",
            "range": "± 0.06698216240694702"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775607244,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1369.4895833333333,
            "unit": "ns",
            "range": "± 407.1651157323015"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 859.5416666666666,
            "unit": "ns",
            "range": "± 285.0034502715128"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1729.3473684210526,
            "unit": "ns",
            "range": "± 350.9160282782416"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 485727.55,
            "unit": "ns",
            "range": "± 81395.53800261318"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1778.0520833333333,
            "unit": "ns",
            "range": "± 454.90199915775196"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 7400.5,
            "unit": "ns",
            "range": "± 31.129382607730296"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1133.1354166666667,
            "unit": "ns",
            "range": "± 435.21509793353715"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 910.7938144329897,
            "unit": "ns",
            "range": "± 366.7306839421452"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1469.625,
            "unit": "ns",
            "range": "± 34.26538583857865"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 449467.15789473685,
            "unit": "ns",
            "range": "± 49234.734899280855"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 1670.1875,
            "unit": "ns",
            "range": "± 481.83149079975647"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 8221.77659574468,
            "unit": "ns",
            "range": "± 630.4811126630564"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 990.8736842105263,
            "unit": "ns",
            "range": "± 428.4010906999162"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 985.3917525773196,
            "unit": "ns",
            "range": "± 306.63980487091277"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1259.0052631578947,
            "unit": "ns",
            "range": "± 421.35318671006615"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 402300.6875,
            "unit": "ns",
            "range": "± 12252.736356494992"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 1389.2777777777778,
            "unit": "ns",
            "range": "± 37.16031335832475"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 8482.294736842105,
            "unit": "ns",
            "range": "± 1026.7415601576588"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 912.0473684210526,
            "unit": "ns",
            "range": "± 544.3524704583097"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 914.0625,
            "unit": "ns",
            "range": "± 339.7598474619513"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1506.6505376344087,
            "unit": "ns",
            "range": "± 677.0982676455619"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 484647.8,
            "unit": "ns",
            "range": "± 10982.94640555841"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1936.59375,
            "unit": "ns",
            "range": "± 453.15152753565195"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 7604.153846153846,
            "unit": "ns",
            "range": "± 91.03922794950002"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1164.0106382978724,
            "unit": "ns",
            "range": "± 333.7246787037666"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 904.875,
            "unit": "ns",
            "range": "± 23.57081528783706"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 1499.5384615384614,
            "unit": "ns",
            "range": "± 30.012817774564766"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 478003.81481481483,
            "unit": "ns",
            "range": "± 13222.539794541502"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 1283.3541666666667,
            "unit": "ns",
            "range": "± 600.1387555200821"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 7678.875,
            "unit": "ns",
            "range": "± 153.62372863591094"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775621080,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 17534.711715698242,
            "unit": "ns",
            "range": "± 18.302160404237053"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 17997.00217328753,
            "unit": "ns",
            "range": "± 51.8780963928735"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 17799.72071126302,
            "unit": "ns",
            "range": "± 120.8870430652041"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775631852,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 35311.021881103516,
            "unit": "ns",
            "range": "± 148.71605621956752"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 36344.50432840983,
            "unit": "ns",
            "range": "± 52.77657672471875"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 32238.362361027645,
            "unit": "ns",
            "range": "± 34.03122187273039"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30777.813807560848,
            "unit": "ns",
            "range": "± 37.04915490566373"
          }
        ]
      }
    ],
    "Operations.BasicOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775642999,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1770.4250938708965,
            "unit": "ns",
            "range": "± 1.8307150812466202"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1744.2580740610758,
            "unit": "ns",
            "range": "± 7.547476345604342"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1742.5452943529401,
            "unit": "ns",
            "range": "± 7.060722951507555"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775643850,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 97369.75497233073,
            "unit": "ns",
            "range": "± 496.32198048750627"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 108099.82500348773,
            "unit": "ns",
            "range": "± 641.5204749052895"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 100867.87223597935,
            "unit": "ns",
            "range": "± 1089.6902923374896"
          }
        ]
      }
    ],
    "Network.BasicOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775660083,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 70.61426560084026,
            "unit": "ns",
            "range": "± 0.16673859917937528"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775724652,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15152.854715983072,
            "unit": "ns",
            "range": "± 19.996636769496867"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15164.084683931791,
            "unit": "ns",
            "range": "± 10.050186803865664"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 15235.072937011719,
            "unit": "ns",
            "range": "± 12.081762014314593"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775743968,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 89687.22882952009,
            "unit": "ns",
            "range": "± 106.25752862828199"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 103380.2657063802,
            "unit": "ns",
            "range": "± 279.9862830518716"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 97320.00470842634,
            "unit": "ns",
            "range": "± 112.00429532760604"
          }
        ]
      }
    ],
    "Network.RawStringOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775741926,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 235.49631841366107,
            "unit": "ns",
            "range": "± 0.3935739475582564"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 272.0014132261276,
            "unit": "ns",
            "range": "± 0.29296891314898144"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 313.6078796784083,
            "unit": "ns",
            "range": "± 0.5983541601400467"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 307.18567810058596,
            "unit": "ns",
            "range": "± 3.0305729658526435"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 233.99666666984558,
            "unit": "ns",
            "range": "± 0.4051189954866054"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 179.93863185246786,
            "unit": "ns",
            "range": "± 0.6878105903785106"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 308.28529250621796,
            "unit": "ns",
            "range": "± 0.15051218044001421"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 305.18683231793915,
            "unit": "ns",
            "range": "± 1.357832907516278"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 350.54391783934375,
            "unit": "ns",
            "range": "± 0.4396477625258671"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 358.11616737501964,
            "unit": "ns",
            "range": "± 1.845024126153674"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775741850,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 35141.78728376116,
            "unit": "ns",
            "range": "± 38.68413934578423"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 36515.82071940104,
            "unit": "ns",
            "range": "± 79.97042679127674"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 30874.530465262276,
            "unit": "ns",
            "range": "± 88.72528530947119"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 29417.594604492188,
            "unit": "ns",
            "range": "± 95.0292913960312"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775772497,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16280.196416219076,
            "unit": "ns",
            "range": "± 70.8777778011"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 15354.856732686361,
            "unit": "ns",
            "range": "± 18.69004962149464"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 15642.309170532226,
            "unit": "ns",
            "range": "± 111.53735579475837"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13520.775224812825,
            "unit": "ns",
            "range": "± 45.03367271657014"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 110486.53499930246,
            "unit": "ns",
            "range": "± 371.527315578428"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19713.9175391564,
            "unit": "ns",
            "range": "± 61.81552328847444"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19170.706975301106,
            "unit": "ns",
            "range": "± 21.808569329791993"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15264.323721749442,
            "unit": "ns",
            "range": "± 67.22591827561403"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 14630.320125579834,
            "unit": "ns",
            "range": "± 9.161068650204154"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 120212.57086588541,
            "unit": "ns",
            "range": "± 647.3604884395313"
          }
        ]
      }
    ],
    "Operations.BasicOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775761599,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1679.0773900349934,
            "unit": "ns",
            "range": "± 2.0911276454542977"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1803.329397837321,
            "unit": "ns",
            "range": "± 2.543195409971087"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1769.4840049743652,
            "unit": "ns",
            "range": "± 2.4069898753451335"
          }
        ]
      }
    ],
    "Operations.CustomOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775807201,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 32539.13138991136,
            "unit": "ns",
            "range": "± 27.10253160419041"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 139231.7865234375,
            "unit": "ns",
            "range": "± 673.9286721701898"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 108902.43671123798,
            "unit": "ns",
            "range": "± 292.81116542617883"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 78978.8459003155,
            "unit": "ns",
            "range": "± 408.7096176250825"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 30423.552736722508,
            "unit": "ns",
            "range": "± 25.626497298767134"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 148559.3182547433,
            "unit": "ns",
            "range": "± 1179.5401954042004"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 119596.83670247396,
            "unit": "ns",
            "range": "± 528.167652415028"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 99941.04227120536,
            "unit": "ns",
            "range": "± 220.31544405853745"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 31725.518803523137,
            "unit": "ns",
            "range": "± 106.87009639716102"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 143242.50463053386,
            "unit": "ns",
            "range": "± 1324.9589987097656"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 113187.58267647879,
            "unit": "ns",
            "range": "± 395.35665361341654"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 75403.28064903847,
            "unit": "ns",
            "range": "± 172.260265053936"
          }
        ]
      }
    ],
    "Network.RawStringOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775896725,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 212.6102374150203,
            "unit": "ns",
            "range": "± 0.26445356146869453"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 271.2041446140834,
            "unit": "ns",
            "range": "± 0.8219331960297244"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 276.8297863006592,
            "unit": "ns",
            "range": "± 0.5752880293402718"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 294.37722609593317,
            "unit": "ns",
            "range": "± 0.501072722959794"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 226.05992317199707,
            "unit": "ns",
            "range": "± 0.3590607328460868"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 154.7416159084865,
            "unit": "ns",
            "range": "± 0.2232041889623697"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 275.7186852968656,
            "unit": "ns",
            "range": "± 0.365755077394988"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 306.6875171661377,
            "unit": "ns",
            "range": "± 0.49731623517016105"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 359.5760481698172,
            "unit": "ns",
            "range": "± 0.7522759611885236"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 331.6696507590158,
            "unit": "ns",
            "range": "± 0.6801292560932376"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775913139,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16378.811863490513,
            "unit": "ns",
            "range": "± 21.94414701058297"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 14849.951700063852,
            "unit": "ns",
            "range": "± 20.617524804342597"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14156.109619140625,
            "unit": "ns",
            "range": "± 13.640679946721816"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13114.596761067709,
            "unit": "ns",
            "range": "± 23.170365895306393"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 106496.33701869419,
            "unit": "ns",
            "range": "± 492.74332203353026"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 20206.126607259113,
            "unit": "ns",
            "range": "± 51.54209048434398"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19849.018147786457,
            "unit": "ns",
            "range": "± 81.77591834106477"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15257.30460030692,
            "unit": "ns",
            "range": "± 20.113632192614396"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 13883.66948445638,
            "unit": "ns",
            "range": "± 124.20002796023898"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 113161.79286411831,
            "unit": "ns",
            "range": "± 256.29696611145556"
          }
        ]
      }
    ],
    "Lua.LuaRunnerOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775916092,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 9330.808080808081,
            "unit": "ns",
            "range": "± 2538.3457881742984"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 11776.767676767677,
            "unit": "ns",
            "range": "± 3085.212534696868"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 372522.9166666667,
            "unit": "ns",
            "range": "± 60098.404356025545"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 398815.306122449,
            "unit": "ns",
            "range": "± 78529.89725771571"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 43273.73737373737,
            "unit": "ns",
            "range": "± 6730.036022867869"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 150080.92783505155,
            "unit": "ns",
            "range": "± 23180.884549670547"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 9822.916666666666,
            "unit": "ns",
            "range": "± 2326.1207447805114"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 11181.632653061224,
            "unit": "ns",
            "range": "± 2495.0919632154178"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 374304.2105263158,
            "unit": "ns",
            "range": "± 75201.08350819338"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 379520.1030927835,
            "unit": "ns",
            "range": "± 72160.16353245694"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 41126.5306122449,
            "unit": "ns",
            "range": "± 6515.41472788746"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 158108.2474226804,
            "unit": "ns",
            "range": "± 27302.650474599424"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 11034,
            "unit": "ns",
            "range": "± 3374.2704411622094"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 9655.102040816326,
            "unit": "ns",
            "range": "± 3775.951370498159"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 376250.5154639175,
            "unit": "ns",
            "range": "± 65137.69409827048"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 359764,
            "unit": "ns",
            "range": "± 82466.67036574303"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 35622.448979591834,
            "unit": "ns",
            "range": "± 11576.625317184777"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 147775.25252525252,
            "unit": "ns",
            "range": "± 27412.60079477209"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 13042.857142857143,
            "unit": "ns",
            "range": "± 2442.577648102575"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 11395.876288659794,
            "unit": "ns",
            "range": "± 2876.4097096674896"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 524389,
            "unit": "ns",
            "range": "± 107145.96566640229"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 531067,
            "unit": "ns",
            "range": "± 118913.22789617171"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 43719.19191919192,
            "unit": "ns",
            "range": "± 9667.478374211778"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 156417.67676767678,
            "unit": "ns",
            "range": "± 25941.64891885534"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 9117.171717171717,
            "unit": "ns",
            "range": "± 4041.5454814186605"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 9304.123711340206,
            "unit": "ns",
            "range": "± 2581.7031493188274"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 487986,
            "unit": "ns",
            "range": "± 101232.83794692253"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 478897,
            "unit": "ns",
            "range": "± 111022.3614340095"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 34767.34693877551,
            "unit": "ns",
            "range": "± 11381.00040454658"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 155592.70833333334,
            "unit": "ns",
            "range": "± 28584.177606323123"
          }
        ]
      }
    ],
    "Operations.CustomOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775942695,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 30827.843366350447,
            "unit": "ns",
            "range": "± 43.58548656472821"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 143936.05631510416,
            "unit": "ns",
            "range": "± 263.64123807125173"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 102037.74695763222,
            "unit": "ns",
            "range": "± 112.93454357095014"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 80654.2497907366,
            "unit": "ns",
            "range": "± 133.0734091913731"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 29099.62240365835,
            "unit": "ns",
            "range": "± 38.887141191136635"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 147139.78620256696,
            "unit": "ns",
            "range": "± 326.91009908796747"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 110825.03313337054,
            "unit": "ns",
            "range": "± 271.16871662474415"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 102010.09521484375,
            "unit": "ns",
            "range": "± 172.39147761152105"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 29294.107759915867,
            "unit": "ns",
            "range": "± 36.68910139919471"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 136113.90729631696,
            "unit": "ns",
            "range": "± 181.58699395107251"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 109445.75907389323,
            "unit": "ns",
            "range": "± 99.35275505227531"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 79268.47359793527,
            "unit": "ns",
            "range": "± 110.31868211530715"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775943224,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1416.6666666666667,
            "unit": "ns",
            "range": "± 1375.8143655811002"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 1044.8275862068965,
            "unit": "ns",
            "range": "± 604.7791487518695"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 2642.8571428571427,
            "unit": "ns",
            "range": "± 1201.956077691033"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 436945.45454545453,
            "unit": "ns",
            "range": "± 96312.1225683341"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 3729.896907216495,
            "unit": "ns",
            "range": "± 2178.4056167132503"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 15898.979591836734,
            "unit": "ns",
            "range": "± 5264.552631701041"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1884.375,
            "unit": "ns",
            "range": "± 1654.2340014577585"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 1274.468085106383,
            "unit": "ns",
            "range": "± 970.9696041013678"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 2274.468085106383,
            "unit": "ns",
            "range": "± 1540.1393715884035"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 418290.8163265306,
            "unit": "ns",
            "range": "± 97156.95993208243"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 3212.5,
            "unit": "ns",
            "range": "± 2119.2973016147544"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 11090.816326530612,
            "unit": "ns",
            "range": "± 4513.2356723261755"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1241.4893617021276,
            "unit": "ns",
            "range": "± 1143.0396727296911"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 990.1098901098901,
            "unit": "ns",
            "range": "± 665.3411731426794"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 2426.0416666666665,
            "unit": "ns",
            "range": "± 1977.5713330226502"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 385134.3373493976,
            "unit": "ns",
            "range": "± 36767.20596695531"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 2644.0860215053763,
            "unit": "ns",
            "range": "± 1601.5931175735611"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 12940.721649484536,
            "unit": "ns",
            "range": "± 4646.707671258283"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1731.6326530612246,
            "unit": "ns",
            "range": "± 1773.1442710431359"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 1166.6666666666667,
            "unit": "ns",
            "range": "± 631.2107146525117"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 2579.1666666666665,
            "unit": "ns",
            "range": "± 1642.521595895304"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 463182.2916666667,
            "unit": "ns",
            "range": "± 95051.00126252629"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 2774.226804123711,
            "unit": "ns",
            "range": "± 2302.683142848227"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 13680.208333333334,
            "unit": "ns",
            "range": "± 4567.96902449786"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1544.0860215053763,
            "unit": "ns",
            "range": "± 1382.496353135484"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 735.6382978723404,
            "unit": "ns",
            "range": "± 729.3620109613819"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 3106.315789473684,
            "unit": "ns",
            "range": "± 1903.010522344082"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 450496.6666666667,
            "unit": "ns",
            "range": "± 64005.457018053436"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 3823.1958762886597,
            "unit": "ns",
            "range": "± 1824.8011001155219"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 10393.617021276596,
            "unit": "ns",
            "range": "± 3933.583239116977"
          }
        ]
      }
    ],
    "Lua.LuaScripts (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775967672,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 298.9808144569397,
            "unit": "ns",
            "range": "± 1.1327876315480474"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 344.2948820407574,
            "unit": "ns",
            "range": "± 0.8270547343365559"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 637.298029092642,
            "unit": "ns",
            "range": "± 0.5480800343384514"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 871.00245997111,
            "unit": "ns",
            "range": "± 1.9381322575641502"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 272.65293682538544,
            "unit": "ns",
            "range": "± 0.28275578073150276"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 373.21669946398055,
            "unit": "ns",
            "range": "± 0.7019336199700087"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 616.7056335449219,
            "unit": "ns",
            "range": "± 2.3632636246312893"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 883.650885105133,
            "unit": "ns",
            "range": "± 1.4120059929206132"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 285.39151334762573,
            "unit": "ns",
            "range": "± 0.34330237236857486"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 355.16830587387085,
            "unit": "ns",
            "range": "± 0.4647353281139208"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 637.1600338390896,
            "unit": "ns",
            "range": "± 2.786736430183249"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 851.0304866790772,
            "unit": "ns",
            "range": "± 1.5456290848094498"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 273.3691965249869,
            "unit": "ns",
            "range": "± 0.21545999505394453"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 345.9326937539237,
            "unit": "ns",
            "range": "± 1.3942541383409555"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 627.5340735753377,
            "unit": "ns",
            "range": "± 2.020241747319268"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 911.208864552634,
            "unit": "ns",
            "range": "± 1.4704485678607604"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 271.5397990300105,
            "unit": "ns",
            "range": "± 0.1933489745251267"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 354.6750092873207,
            "unit": "ns",
            "range": "± 0.62227646297919"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 637.0675131252834,
            "unit": "ns",
            "range": "± 1.7038208482288266"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 877.4792491367886,
            "unit": "ns",
            "range": "± 0.9915490414056423"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747775970976,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 29744.62138977051,
            "unit": "ns",
            "range": "± 89.045409831338"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 38110.27780386118,
            "unit": "ns",
            "range": "± 143.69844834455418"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 53541.941947428386,
            "unit": "ns",
            "range": "± 292.5908882289311"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 60627.123018391925,
            "unit": "ns",
            "range": "± 196.14957978863552"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 15106.638805135091,
            "unit": "ns",
            "range": "± 43.5069989336898"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 27567.068381754558,
            "unit": "ns",
            "range": "± 85.87243743093839"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 28792.097268676756,
            "unit": "ns",
            "range": "± 97.93942824671869"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 43224.234540666854,
            "unit": "ns",
            "range": "± 147.04885914746018"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 61268.22526448568,
            "unit": "ns",
            "range": "± 392.44212449836033"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 57043.57277018229,
            "unit": "ns",
            "range": "± 198.18069851124469"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15291.262034098307,
            "unit": "ns",
            "range": "± 77.47900457432634"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 32697.499709065756,
            "unit": "ns",
            "range": "± 125.11494425633899"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 28156.11361796061,
            "unit": "ns",
            "range": "± 83.51434675266079"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 38653.66835021973,
            "unit": "ns",
            "range": "± 52.093542821690335"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 55603.31498936244,
            "unit": "ns",
            "range": "± 121.26096622529856"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 59125.909993489586,
            "unit": "ns",
            "range": "± 127.43538039833273"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 15187.768693033855,
            "unit": "ns",
            "range": "± 49.58222149303368"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 27019.95475064791,
            "unit": "ns",
            "range": "± 29.980196782104947"
          }
        ]
      }
    ],
    "Lua.LuaScripts (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776037771,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 158.24580589930216,
            "unit": "ns",
            "range": "± 0.1820957426438672"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 208.76084486643472,
            "unit": "ns",
            "range": "± 0.5207393255905374"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 322.7869306291853,
            "unit": "ns",
            "range": "± 1.4698112192622537"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 370.42740072522844,
            "unit": "ns",
            "range": "± 0.5627041024208699"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 164.08304373423258,
            "unit": "ns",
            "range": "± 0.39703917200662014"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 195.86904366811117,
            "unit": "ns",
            "range": "± 0.7635532990985131"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 299.19370242527555,
            "unit": "ns",
            "range": "± 1.381523848347149"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 354.46006457010907,
            "unit": "ns",
            "range": "± 0.5440171014348875"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 161.95298773901803,
            "unit": "ns",
            "range": "± 0.3339317693377262"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 196.09760602315268,
            "unit": "ns",
            "range": "± 0.46729728434818907"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 310.1927439371745,
            "unit": "ns",
            "range": "± 1.4614003212301472"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 352.75481087820873,
            "unit": "ns",
            "range": "± 0.8924869473871523"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 157.17754534312658,
            "unit": "ns",
            "range": "± 0.20371633311135198"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 204.93364334106445,
            "unit": "ns",
            "range": "± 0.32859134936402445"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 315.9132855279105,
            "unit": "ns",
            "range": "± 0.6010083951523264"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 366.49694783346996,
            "unit": "ns",
            "range": "± 0.6444968414760205"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 157.68324307032995,
            "unit": "ns",
            "range": "± 0.15458416884484907"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 187.08518835214468,
            "unit": "ns",
            "range": "± 0.27759003896725126"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 326.2066534587315,
            "unit": "ns",
            "range": "± 0.49583605679643317"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 361.6115019871638,
            "unit": "ns",
            "range": "± 0.27703734203305536"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776047831,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 31325.65424992488,
            "unit": "ns",
            "range": "± 57.980302393904026"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 48348.865966796875,
            "unit": "ns",
            "range": "± 105.63862600451714"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 65834.43603515625,
            "unit": "ns",
            "range": "± 170.51451168467625"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 49134.946986607145,
            "unit": "ns",
            "range": "± 84.30310547374268"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 16830.462210518974,
            "unit": "ns",
            "range": "± 61.42100721842221"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 26573.187561035156,
            "unit": "ns",
            "range": "± 45.13962347646252"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 32064.567347935266,
            "unit": "ns",
            "range": "± 87.24497432675322"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 53709.5206124442,
            "unit": "ns",
            "range": "± 104.4059956182574"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 69419.7476900541,
            "unit": "ns",
            "range": "± 221.13092703880227"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 49996.851399739586,
            "unit": "ns",
            "range": "± 122.40724996563205"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15977.33677455357,
            "unit": "ns",
            "range": "± 52.53029503043127"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 30522.801310221355,
            "unit": "ns",
            "range": "± 159.4336155560572"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 32531.082560221355,
            "unit": "ns",
            "range": "± 90.33802765099384"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 47469.1170828683,
            "unit": "ns",
            "range": "± 59.528961338237565"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 65253.108723958336,
            "unit": "ns",
            "range": "± 188.74446837933928"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 48632.77404785156,
            "unit": "ns",
            "range": "± 125.52279192785768"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 16579.94435628255,
            "unit": "ns",
            "range": "± 46.35997123913565"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 26002.789916992188,
            "unit": "ns",
            "range": "± 148.60778197885136"
          }
        ]
      }
    ],
    "Operations.JsonOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776114953,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 133720.78857421875,
            "unit": "ns",
            "range": "± 529.1988419965948"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 119510.4243351863,
            "unit": "ns",
            "range": "± 234.42582472715964"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: ACL)",
            "value": 144293.52504185267,
            "unit": "ns",
            "range": "± 433.11316028949125"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: ACL)",
            "value": 265138.7646484375,
            "unit": "ns",
            "range": "± 796.053869889565"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: ACL)",
            "value": 4704.994964599609,
            "unit": "ns",
            "range": "± 9.610218677267651"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: ACL)",
            "value": 274847.2393329327,
            "unit": "ns",
            "range": "± 807.3360281494055"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: ACL)",
            "value": 8078060.9375,
            "unit": "ns",
            "range": "± 42889.822085976375"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 137411.20849609375,
            "unit": "ns",
            "range": "± 209.25659373836467"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 129426.88802083333,
            "unit": "ns",
            "range": "± 452.8530886007167"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: AOF)",
            "value": 143980.09127103366,
            "unit": "ns",
            "range": "± 391.6311600856649"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: AOF)",
            "value": 264949.8360770089,
            "unit": "ns",
            "range": "± 566.4678173190306"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: AOF)",
            "value": 4763.957377842495,
            "unit": "ns",
            "range": "± 16.409970421115737"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: AOF)",
            "value": 275199.49951171875,
            "unit": "ns",
            "range": "± 395.62942577189176"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: AOF)",
            "value": 8176098.28125,
            "unit": "ns",
            "range": "± 32709.03859235488"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: None)",
            "value": 134555.07114955358,
            "unit": "ns",
            "range": "± 551.8802238464841"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: None)",
            "value": 117103.72750418527,
            "unit": "ns",
            "range": "± 420.4270295952914"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: None)",
            "value": 137691.56494140625,
            "unit": "ns",
            "range": "± 372.69815893058814"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: None)",
            "value": 265657.34299879806,
            "unit": "ns",
            "range": "± 399.10672528416467"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: None)",
            "value": 4789.579244760366,
            "unit": "ns",
            "range": "± 12.417104126942723"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: None)",
            "value": 277485.5354817708,
            "unit": "ns",
            "range": "± 800.6793168146527"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: None)",
            "value": 8201324.166666667,
            "unit": "ns",
            "range": "± 17200.81114891439"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776155366,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 14330.864166259766,
            "unit": "ns",
            "range": "± 24.602272103537267"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 20379.661686488562,
            "unit": "ns",
            "range": "± 72.78824651410838"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 19835.111880493165,
            "unit": "ns",
            "range": "± 101.98943767224145"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 21299.11938694545,
            "unit": "ns",
            "range": "± 106.84487549177271"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15857.796595646785,
            "unit": "ns",
            "range": "± 38.35045056694432"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 9980.849056537334,
            "unit": "ns",
            "range": "± 12.750872733374633"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 20784.152788798016,
            "unit": "ns",
            "range": "± 15.417211227550755"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 20952.949180603027,
            "unit": "ns",
            "range": "± 10.607137824975274"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 26497.127235921223,
            "unit": "ns",
            "range": "± 102.73575239358463"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 27059.12510211651,
            "unit": "ns",
            "range": "± 31.303342253540006"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 21415.983779907227,
            "unit": "ns",
            "range": "± 135.65935988256962"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26086.358986990792,
            "unit": "ns",
            "range": "± 93.83881500230241"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 28632.394479370116,
            "unit": "ns",
            "range": "± 123.58444710105154"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 31974.763417561848,
            "unit": "ns",
            "range": "± 151.86862761470903"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 16055.928932189941,
            "unit": "ns",
            "range": "± 8.588989404768867"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10029.717422993977,
            "unit": "ns",
            "range": "± 55.32531485451037"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 27367.298580932616,
            "unit": "ns",
            "range": "± 90.97788502031409"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 26984.370704064004,
            "unit": "ns",
            "range": "± 51.434565584091956"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 32187.707580566406,
            "unit": "ns",
            "range": "± 115.03667638553756"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 31954.431675502234,
            "unit": "ns",
            "range": "± 180.5667911583725"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14152.878019060407,
            "unit": "ns",
            "range": "± 45.86850987388378"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 20902.940368652344,
            "unit": "ns",
            "range": "± 28.747382705927922"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 21380.86652832031,
            "unit": "ns",
            "range": "± 93.99732252730055"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 21842.98024749756,
            "unit": "ns",
            "range": "± 27.439802097458976"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15776.683295694987,
            "unit": "ns",
            "range": "± 79.77716770320406"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10417.317775472005,
            "unit": "ns",
            "range": "± 69.04374380440497"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 21379.724678548177,
            "unit": "ns",
            "range": "± 115.56852082377874"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 21764.051394144695,
            "unit": "ns",
            "range": "± 14.721694393138872"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 27030.737204415458,
            "unit": "ns",
            "range": "± 74.54665765806433"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 25125.13697378976,
            "unit": "ns",
            "range": "± 78.05063124648329"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776307718,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 151405.2573765346,
            "unit": "ns",
            "range": "± 552.2871365216114"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 18649.802295391375,
            "unit": "ns",
            "range": "± 15.481482622666858"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 17535.732025146484,
            "unit": "ns",
            "range": "± 60.82974639569593"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 154580.92392202525,
            "unit": "ns",
            "range": "± 188.6072003848973"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 46808.79009602864,
            "unit": "ns",
            "range": "± 131.43005327700777"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 135198.8814174107,
            "unit": "ns",
            "range": "± 187.41949697618574"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 10395831.234375,
            "unit": "ns",
            "range": "± 142930.3482379233"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 295275.6761343149,
            "unit": "ns",
            "range": "± 13614.65154051905"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 151294.7378580729,
            "unit": "ns",
            "range": "± 976.0599582961764"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 18465.573492867607,
            "unit": "ns",
            "range": "± 106.41479332909346"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 16548.91453552246,
            "unit": "ns",
            "range": "± 8.844104188755772"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 152971.2831655649,
            "unit": "ns",
            "range": "± 763.0781443593381"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 45081.98016793387,
            "unit": "ns",
            "range": "± 54.0055977120716"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 131312.76075157753,
            "unit": "ns",
            "range": "± 83.05019254027778"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 10465332.30357143,
            "unit": "ns",
            "range": "± 94932.01095300577"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 292628.65365658165,
            "unit": "ns",
            "range": "± 12167.970223182921"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 148790.27332481972,
            "unit": "ns",
            "range": "± 355.8101715046211"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 18606.412192711465,
            "unit": "ns",
            "range": "± 12.368728241721135"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 17207.111264038085,
            "unit": "ns",
            "range": "± 73.28013063218414"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 157644.0092610677,
            "unit": "ns",
            "range": "± 833.7111594451168"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 47321.62774222238,
            "unit": "ns",
            "range": "± 111.3030353748631"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 128703.56719501202,
            "unit": "ns",
            "range": "± 113.8463744885855"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 8552445.890024038,
            "unit": "ns",
            "range": "± 40661.284460117575"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 265682.85017277644,
            "unit": "ns",
            "range": "± 230.73504363112247"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 154237.825390625,
            "unit": "ns",
            "range": "± 703.5397631202482"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 18627.23662414551,
            "unit": "ns",
            "range": "± 127.7284884301401"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 17152.422615559895,
            "unit": "ns",
            "range": "± 78.31230240732283"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 156116.39858774038,
            "unit": "ns",
            "range": "± 254.85496402112682"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 46347.473901367186,
            "unit": "ns",
            "range": "± 222.2924951107139"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 132447.90727539064,
            "unit": "ns",
            "range": "± 417.149494805096"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 9498961.46875,
            "unit": "ns",
            "range": "± 61047.388437027614"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 288851.93994140625,
            "unit": "ns",
            "range": "± 919.1964555329203"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 151366.52015904017,
            "unit": "ns",
            "range": "± 581.9289437577628"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 19217.397794087727,
            "unit": "ns",
            "range": "± 8.565826047861245"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 17077.909025065102,
            "unit": "ns",
            "range": "± 74.24583680409575"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 153280.4895891462,
            "unit": "ns",
            "range": "± 109.00962594553147"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 46239.027779715405,
            "unit": "ns",
            "range": "± 47.28250335427551"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 135189.49295247396,
            "unit": "ns",
            "range": "± 539.1037260331073"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 9385154.796875,
            "unit": "ns",
            "range": "± 48168.24308304013"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 290403.8205240885,
            "unit": "ns",
            "range": "± 725.4474778974494"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776387123,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 13490.348161969867,
            "unit": "ns",
            "range": "± 28.74366258941166"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 19661.42796107701,
            "unit": "ns",
            "range": "± 38.45983504675138"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 22478.32743326823,
            "unit": "ns",
            "range": "± 267.38790777386464"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 22942.04581124442,
            "unit": "ns",
            "range": "± 29.02955537903666"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15700.112043108258,
            "unit": "ns",
            "range": "± 46.4202636814767"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 9725.775255475726,
            "unit": "ns",
            "range": "± 31.960159520429883"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 22268.6675008138,
            "unit": "ns",
            "range": "± 32.05955432927807"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 22553.00574669471,
            "unit": "ns",
            "range": "± 29.803585892624653"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 26672.54098745493,
            "unit": "ns",
            "range": "± 38.34146725574286"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 25690.3998819987,
            "unit": "ns",
            "range": "± 201.84408987158474"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 19891.037691556492,
            "unit": "ns",
            "range": "± 38.62724657438476"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 24674.693516322546,
            "unit": "ns",
            "range": "± 50.547869453135306"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 29262.826538085938,
            "unit": "ns",
            "range": "± 122.71180834064502"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 27657.506016322546,
            "unit": "ns",
            "range": "± 60.613059231723476"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15345.057896205357,
            "unit": "ns",
            "range": "± 18.85282568794295"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 9634.919847760882,
            "unit": "ns",
            "range": "± 19.51769627850163"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 27485.618693033855,
            "unit": "ns",
            "range": "± 34.6297451325755"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 28307.076808384485,
            "unit": "ns",
            "range": "± 47.821499536863286"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 31473.212280273438,
            "unit": "ns",
            "range": "± 210.62599370385658"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 31623.298950195312,
            "unit": "ns",
            "range": "± 166.51063384011488"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14036.150796072823,
            "unit": "ns",
            "range": "± 21.431874255684217"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19230.94438825335,
            "unit": "ns",
            "range": "± 34.07840917309949"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 20561.097599909855,
            "unit": "ns",
            "range": "± 50.638760608912506"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 20662.776438395184,
            "unit": "ns",
            "range": "± 36.45913192651834"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15699.728495279947,
            "unit": "ns",
            "range": "± 26.013867551939764"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 9812.70505464994,
            "unit": "ns",
            "range": "± 10.404560482570423"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 21317.276654924666,
            "unit": "ns",
            "range": "± 44.920315447157556"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 21351.094709123885,
            "unit": "ns",
            "range": "± 48.924152588302945"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 24234.189932686942,
            "unit": "ns",
            "range": "± 22.425798934667664"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 26013.632202148438,
            "unit": "ns",
            "range": "± 34.38423197827228"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776592659,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 98094.40611921038,
            "unit": "ns",
            "range": "± 393.32359627939036"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 11421.858024597168,
            "unit": "ns",
            "range": "± 10.235227553356534"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11352.872043064663,
            "unit": "ns",
            "range": "± 44.096731645320574"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10279.0860548753,
            "unit": "ns",
            "range": "± 12.700968974074494"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 12758.296732584635,
            "unit": "ns",
            "range": "± 41.0733991077052"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 13068.155475323018,
            "unit": "ns",
            "range": "± 9.046235989711597"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 11194.566655476889,
            "unit": "ns",
            "range": "± 10.924547625777137"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10066.44465637207,
            "unit": "ns",
            "range": "± 8.281015967901487"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 12624.726929297814,
            "unit": "ns",
            "range": "± 17.183627506794117"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13534.859359232585,
            "unit": "ns",
            "range": "± 45.299774854700644"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 11453.640926106771,
            "unit": "ns",
            "range": "± 30.96201843154763"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 5222.020648701986,
            "unit": "ns",
            "range": "± 29.707014496310702"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12577.765811920166,
            "unit": "ns",
            "range": "± 6.094292507932755"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 12405.586450703939,
            "unit": "ns",
            "range": "± 55.529946117853335"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 11498.695458730062,
            "unit": "ns",
            "range": "± 9.950263453643675"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 115117.81257847378,
            "unit": "ns",
            "range": "± 244.0480212697126"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 50430.277701241626,
            "unit": "ns",
            "range": "± 152.68711037717432"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 51450.79533284505,
            "unit": "ns",
            "range": "± 159.01058561893606"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 54785.43966878255,
            "unit": "ns",
            "range": "± 164.65119833438342"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 64420.21558837891,
            "unit": "ns",
            "range": "± 278.3918055229261"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 92339.65918907753,
            "unit": "ns",
            "range": "± 212.38259169092174"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 54658.73688761393,
            "unit": "ns",
            "range": "± 159.41393760894098"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 46572.03504356971,
            "unit": "ns",
            "range": "± 57.27785667893713"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 56653.47501220703,
            "unit": "ns",
            "range": "± 131.01580423011262"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 68271.10545247396,
            "unit": "ns",
            "range": "± 407.58390254440116"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 59684.000915527344,
            "unit": "ns",
            "range": "± 75.06342427539686"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 5239.538500976562,
            "unit": "ns",
            "range": "± 17.59083586241347"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 57161.75686645508,
            "unit": "ns",
            "range": "± 170.39327505566175"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 51417.50621948242,
            "unit": "ns",
            "range": "± 117.62955843496398"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 55439.23101196289,
            "unit": "ns",
            "range": "± 100.57580150361491"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 107435.85314941406,
            "unit": "ns",
            "range": "± 418.5558301605573"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 51619.92910563151,
            "unit": "ns",
            "range": "± 98.20073807961968"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 51079.417271205355,
            "unit": "ns",
            "range": "± 102.38115316100587"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 55131.29134695871,
            "unit": "ns",
            "range": "± 91.79404339646574"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 58439.37069498698,
            "unit": "ns",
            "range": "± 164.46078200171004"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 83317.04611440805,
            "unit": "ns",
            "range": "± 68.94773577429275"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 52891.920087541854,
            "unit": "ns",
            "range": "± 155.88133438329115"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 49467.41838785807,
            "unit": "ns",
            "range": "± 125.23028820910281"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 58385.86462605794,
            "unit": "ns",
            "range": "± 149.49952643855286"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 58320.378910319014,
            "unit": "ns",
            "range": "± 254.46000528816657"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 59277.78756713867,
            "unit": "ns",
            "range": "± 137.54032990608795"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 5250.443924967448,
            "unit": "ns",
            "range": "± 15.488622445237732"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 50908.50160929362,
            "unit": "ns",
            "range": "± 249.2208432751093"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 50348.0348815918,
            "unit": "ns",
            "range": "± 212.2762921461766"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 54473.2796468099,
            "unit": "ns",
            "range": "± 109.8527885731731"
          }
        ]
      }
    ],
    "Operations.SetOperations (ubuntu-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776643033,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 119819.61672363282,
            "unit": "ns",
            "range": "± 807.89905104365"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 55818.24354201097,
            "unit": "ns",
            "range": "± 145.94994963960983"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10280.311435953776,
            "unit": "ns",
            "range": "± 37.629199159546936"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 11399.973977152507,
            "unit": "ns",
            "range": "± 52.13082320009805"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 25922.321672566733,
            "unit": "ns",
            "range": "± 68.94253081091182"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 12116.011581420898,
            "unit": "ns",
            "range": "± 44.322102081072615"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 13833.508155822754,
            "unit": "ns",
            "range": "± 29.857086002693784"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 11964.261351521809,
            "unit": "ns",
            "range": "± 8.766804536166948"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 11500.221572875977,
            "unit": "ns",
            "range": "± 17.232699106789322"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 12360.51686299642,
            "unit": "ns",
            "range": "± 40.82871064481453"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 13841.81914637639,
            "unit": "ns",
            "range": "± 11.14644842715215"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 12775.20082244873,
            "unit": "ns",
            "range": "± 49.72683858848621"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 14298.210593087333,
            "unit": "ns",
            "range": "± 42.305397947222815"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 14082.125199381511,
            "unit": "ns",
            "range": "± 50.72140692486491"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 12415.493980407715,
            "unit": "ns",
            "range": "± 47.15325874805904"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 13390.119360605875,
            "unit": "ns",
            "range": "± 6.89028036721806"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 130527.02904459635,
            "unit": "ns",
            "range": "± 1346.3943217915219"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 138885.7694091797,
            "unit": "ns",
            "range": "± 792.364998869564"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 46942.236139933266,
            "unit": "ns",
            "range": "± 81.5810225870694"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 53239.08442179362,
            "unit": "ns",
            "range": "± 141.51008508118008"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 250462.46187918526,
            "unit": "ns",
            "range": "± 843.8457959471668"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 54601.491986955916,
            "unit": "ns",
            "range": "± 186.98832354224706"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 59818.5732421875,
            "unit": "ns",
            "range": "± 138.1751245358979"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 59769.52282918294,
            "unit": "ns",
            "range": "± 170.92005389237696"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 67440.17212785993,
            "unit": "ns",
            "range": "± 219.6711579891027"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 157797.1830891927,
            "unit": "ns",
            "range": "± 1165.8663436286129"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 236340.81753976006,
            "unit": "ns",
            "range": "± 1050.961054436855"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 156375.3760811942,
            "unit": "ns",
            "range": "± 709.6940012863064"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 224309.14065987724,
            "unit": "ns",
            "range": "± 803.6566963633278"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 152255.75102539064,
            "unit": "ns",
            "range": "± 784.6585507255447"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 154773.74926757812,
            "unit": "ns",
            "range": "± 1167.0165623176492"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 226900.40524088542,
            "unit": "ns",
            "range": "± 1667.95925472278"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 120305.5657063802,
            "unit": "ns",
            "range": "± 1061.634451612422"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 132756.7481608073,
            "unit": "ns",
            "range": "± 923.0454568740049"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 46335.38524576823,
            "unit": "ns",
            "range": "± 136.9882937362553"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 53693.999892171225,
            "unit": "ns",
            "range": "± 152.89377460558975"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 229862.4762311663,
            "unit": "ns",
            "range": "± 913.4689125712446"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 52380.17198768029,
            "unit": "ns",
            "range": "± 37.47208141001155"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 61718.897982083836,
            "unit": "ns",
            "range": "± 221.5932779296523"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 60199.97241414388,
            "unit": "ns",
            "range": "± 96.41603053668051"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 66164.34454345703,
            "unit": "ns",
            "range": "± 192.98856789604605"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 151633.06036783854,
            "unit": "ns",
            "range": "± 722.2608146647732"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 192951.26767578124,
            "unit": "ns",
            "range": "± 715.2885476834725"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 141876.8500488281,
            "unit": "ns",
            "range": "± 604.9730131704986"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 185129.6173014323,
            "unit": "ns",
            "range": "± 927.2398683233624"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 137021.5317545573,
            "unit": "ns",
            "range": "± 572.5335879399712"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 145307.33824869792,
            "unit": "ns",
            "range": "± 911.8645927433857"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 194815.4780110677,
            "unit": "ns",
            "range": "± 649.1173370680879"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776681924,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 90850.76904296875,
            "unit": "ns",
            "range": "± 432.509236526536"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 24761.4985874721,
            "unit": "ns",
            "range": "± 34.80875550897372"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 23399.151175362724,
            "unit": "ns",
            "range": "± 28.347955004581653"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 77699.07592773438,
            "unit": "ns",
            "range": "± 131.43063260211073"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 31148.644147600447,
            "unit": "ns",
            "range": "± 68.80844493246518"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 77622.7714029948,
            "unit": "ns",
            "range": "± 1326.86698617841"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 5576088.975694444,
            "unit": "ns",
            "range": "± 109042.89708131741"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 156893.9111328125,
            "unit": "ns",
            "range": "± 14139.44994710155"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 92683.57403094952,
            "unit": "ns",
            "range": "± 237.4021878007759"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 24976.228768484933,
            "unit": "ns",
            "range": "± 14.776511241568809"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 23415.656026204426,
            "unit": "ns",
            "range": "± 15.104608756006177"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 75058.28159877232,
            "unit": "ns",
            "range": "± 97.62547961465971"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 32031.432291666668,
            "unit": "ns",
            "range": "± 45.4006609308717"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 75034.32758037861,
            "unit": "ns",
            "range": "± 152.78067667798067"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 5513467.516447368,
            "unit": "ns",
            "range": "± 111813.05169468187"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 153850.0458984375,
            "unit": "ns",
            "range": "± 12851.643611612415"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 91606.61010742188,
            "unit": "ns",
            "range": "± 431.34677689099726"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 25514.245823451452,
            "unit": "ns",
            "range": "± 36.276436301354686"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 23398.63250732422,
            "unit": "ns",
            "range": "± 15.546932733816062"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 75329.05883789062,
            "unit": "ns",
            "range": "± 148.81341816483928"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 31220.789882114954,
            "unit": "ns",
            "range": "± 37.94498183740396"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 76713.06559244792,
            "unit": "ns",
            "range": "± 264.34695805406994"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 4530094.270833333,
            "unit": "ns",
            "range": "± 6961.9279441633"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 142239.404296875,
            "unit": "ns",
            "range": "± 185.84738990959394"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 91185.86344401042,
            "unit": "ns",
            "range": "± 210.60084349497265"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 24833.431788853235,
            "unit": "ns",
            "range": "± 20.02856563221062"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 23611.851196289062,
            "unit": "ns",
            "range": "± 24.185931057525277"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 77763.76342773438,
            "unit": "ns",
            "range": "± 115.48838177625368"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 32750.23193359375,
            "unit": "ns",
            "range": "± 26.33510439077542"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 80925.06917317708,
            "unit": "ns",
            "range": "± 382.27348278203397"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 4982901.021634615,
            "unit": "ns",
            "range": "± 13667.154785562247"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 158691.04817708334,
            "unit": "ns",
            "range": "± 830.004338816274"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 91968.84852818081,
            "unit": "ns",
            "range": "± 171.15866535096154"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 24995.02912248884,
            "unit": "ns",
            "range": "± 59.324239085994094"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 23364.557698567707,
            "unit": "ns",
            "range": "± 15.993055217170227"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 77198.58968098958,
            "unit": "ns",
            "range": "± 84.85184878814727"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 30971.820068359375,
            "unit": "ns",
            "range": "± 41.892463929933506"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 77985.69173177083,
            "unit": "ns",
            "range": "± 181.7477352492279"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 5111411.328125,
            "unit": "ns",
            "range": "± 9779.781202686767"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 157921.07421875,
            "unit": "ns",
            "range": "± 413.204405622685"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776763242,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 98487.69124348958,
            "unit": "ns",
            "range": "± 413.8924235930816"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 12182.647470327523,
            "unit": "ns",
            "range": "± 7.786236039209722"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 10836.916097005209,
            "unit": "ns",
            "range": "± 10.349415075309876"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10461.765543619791,
            "unit": "ns",
            "range": "± 15.745184355088073"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 14683.13217163086,
            "unit": "ns",
            "range": "± 9.896807651385476"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 15198.082071940104,
            "unit": "ns",
            "range": "± 18.140293311086143"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 13198.618825276693,
            "unit": "ns",
            "range": "± 17.534116238423675"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 9732.403153639574,
            "unit": "ns",
            "range": "± 9.306205209323197"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 13404.67514038086,
            "unit": "ns",
            "range": "± 11.371781230795511"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 12844.593048095703,
            "unit": "ns",
            "range": "± 28.061166229938006"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 14182.682139078775,
            "unit": "ns",
            "range": "± 11.914958747986644"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 4449.868927001953,
            "unit": "ns",
            "range": "± 6.065071833529138"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12226.19400024414,
            "unit": "ns",
            "range": "± 11.54295706131721"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 15339.853341238839,
            "unit": "ns",
            "range": "± 17.135140078184246"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 14331.117189847506,
            "unit": "ns",
            "range": "± 12.149104204942056"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 107746.0205078125,
            "unit": "ns",
            "range": "± 236.88307921093326"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 39641.894095284595,
            "unit": "ns",
            "range": "± 57.046154136064594"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 38599.70005580357,
            "unit": "ns",
            "range": "± 47.76452199045457"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 42370.052228655135,
            "unit": "ns",
            "range": "± 41.79867690561626"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 61192.508951822914,
            "unit": "ns",
            "range": "± 325.6647129813093"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 87130.04028320312,
            "unit": "ns",
            "range": "± 183.10372707737523"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 45809.12606375558,
            "unit": "ns",
            "range": "± 61.20722633079499"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 33896.02614182692,
            "unit": "ns",
            "range": "± 89.07417562640987"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 45231.490071614586,
            "unit": "ns",
            "range": "± 121.32436094468305"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 60734.46329752604,
            "unit": "ns",
            "range": "± 227.4868649273248"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 49439.582707331734,
            "unit": "ns",
            "range": "± 57.81022392519237"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 4495.951491135817,
            "unit": "ns",
            "range": "± 4.644914644781625"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 51950.12468610491,
            "unit": "ns",
            "range": "± 120.33169584961826"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 42492.36999511719,
            "unit": "ns",
            "range": "± 57.990826986477934"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 44245.27799166166,
            "unit": "ns",
            "range": "± 27.287349872858766"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 94281.45845853366,
            "unit": "ns",
            "range": "± 83.05497276572729"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 40084.284855769234,
            "unit": "ns",
            "range": "± 48.116555584771355"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 39066.3327730619,
            "unit": "ns",
            "range": "± 64.01388412309385"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 43803.28328450521,
            "unit": "ns",
            "range": "± 41.497698117081015"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 57247.34366280692,
            "unit": "ns",
            "range": "± 32.47905124632543"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 78980.34159342448,
            "unit": "ns",
            "range": "± 75.050680322268"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 44350.01607259115,
            "unit": "ns",
            "range": "± 48.77461999814316"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 34559.383283342635,
            "unit": "ns",
            "range": "± 22.954807987789515"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 46204.62247408353,
            "unit": "ns",
            "range": "± 62.205453143329095"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 51627.321213942305,
            "unit": "ns",
            "range": "± 66.05644259200399"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 50997.94372558594,
            "unit": "ns",
            "range": "± 68.08249542018564"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 4511.089579264323,
            "unit": "ns",
            "range": "± 5.814754968010918"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 45905.914306640625,
            "unit": "ns",
            "range": "± 60.39084845094848"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 42311.96943010603,
            "unit": "ns",
            "range": "± 109.87839055200236"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 44002.00256347656,
            "unit": "ns",
            "range": "± 47.56629155313585"
          }
        ]
      }
    ],
    "Operations.SetOperations (windows-latest  net9.0 Release)": [
      {
        "commit": {
          "author": {
            "email": "darrenge@microsoft.com",
            "name": "darrenge",
            "username": "darrenge"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "febbd1897465aef20267c23905c242338afa7701",
          "message": "Fix BDN scaling issue so that each framework is saved to different branch (#1203)\n\n* Updated the gh-pages to choose different branch for continous branch based on framework\n\n* Update deploy-website.yml\n\n* Move the branch for the data file to the matrix setting\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-05-20T14:10:31-07:00",
          "tree_id": "7905811cbd61b699cac113c1ffeab230bdfa419a",
          "url": "https://github.com/microsoft/garnet/commit/febbd1897465aef20267c23905c242338afa7701"
        },
        "date": 1747776914396,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 112461.70131138393,
            "unit": "ns",
            "range": "± 376.34051786699416"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 59589.05904134115,
            "unit": "ns",
            "range": "± 310.4185556225954"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10365.669686453683,
            "unit": "ns",
            "range": "± 9.149232787301708"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 13509.782233605018,
            "unit": "ns",
            "range": "± 16.15277122782814"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 28521.734971266527,
            "unit": "ns",
            "range": "± 22.70904156189661"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 13858.632441929409,
            "unit": "ns",
            "range": "± 14.435924511009722"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 18456.33261544364,
            "unit": "ns",
            "range": "± 20.743443689360838"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 16411.275423490086,
            "unit": "ns",
            "range": "± 11.858741671006344"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 12012.34852717473,
            "unit": "ns",
            "range": "± 15.667291507620188"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 13964.884244478666,
            "unit": "ns",
            "range": "± 11.308195657663381"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 18746.287536621094,
            "unit": "ns",
            "range": "± 30.45454721746584"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 15250.190633138021,
            "unit": "ns",
            "range": "± 29.10388471098295"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 20017.134748186385,
            "unit": "ns",
            "range": "± 38.44122601679369"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 19549.78309044471,
            "unit": "ns",
            "range": "± 16.808399714865207"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 15004.208246866861,
            "unit": "ns",
            "range": "± 14.714684866394858"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 16501.36444091797,
            "unit": "ns",
            "range": "± 17.140584426754366"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 119910.83658854167,
            "unit": "ns",
            "range": "± 674.3930052106997"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 126657.76936848958,
            "unit": "ns",
            "range": "± 620.8906626527066"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 33470.83536783854,
            "unit": "ns",
            "range": "± 42.12712754553161"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 42276.62916917067,
            "unit": "ns",
            "range": "± 73.74642165700692"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 195791.44944411056,
            "unit": "ns",
            "range": "± 635.496233962875"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 42628.948974609375,
            "unit": "ns",
            "range": "± 91.4063074093974"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 54518.30095563616,
            "unit": "ns",
            "range": "± 109.436485029761"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 51210.28703962053,
            "unit": "ns",
            "range": "± 37.941832087953074"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 53073.04120744978,
            "unit": "ns",
            "range": "± 130.50133648108573"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 124784.716796875,
            "unit": "ns",
            "range": "± 893.8852028407375"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 204063.3650716146,
            "unit": "ns",
            "range": "± 2508.2152821395034"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 117022.29125976562,
            "unit": "ns",
            "range": "± 599.2357809318366"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 189450.31005859375,
            "unit": "ns",
            "range": "± 816.6761589251665"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 121827.08914620536,
            "unit": "ns",
            "range": "± 276.35589301163554"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 117714.75568498884,
            "unit": "ns",
            "range": "± 775.0680912655338"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 186100.9296123798,
            "unit": "ns",
            "range": "± 791.9534720934429"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 113532.76018415179,
            "unit": "ns",
            "range": "± 461.8209607423579"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 120991.78748497597,
            "unit": "ns",
            "range": "± 323.2429595874215"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 35101.898193359375,
            "unit": "ns",
            "range": "± 93.20202855740516"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 43681.530325753345,
            "unit": "ns",
            "range": "± 82.72489718280586"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 172698.0900065104,
            "unit": "ns",
            "range": "± 440.6833091683057"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 41280.09878305288,
            "unit": "ns",
            "range": "± 61.23880112348509"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 52711.024693080355,
            "unit": "ns",
            "range": "± 76.10695222852426"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 50107.81903948103,
            "unit": "ns",
            "range": "± 95.43398339558678"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 55299.79919433594,
            "unit": "ns",
            "range": "± 100.87933833172552"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 117170.361328125,
            "unit": "ns",
            "range": "± 457.0582797291973"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 168613.2722981771,
            "unit": "ns",
            "range": "± 1458.5876425855731"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 109878.54191706731,
            "unit": "ns",
            "range": "± 298.25965327434153"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 148540.29017857142,
            "unit": "ns",
            "range": "± 492.33123675657794"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 114091.83302659255,
            "unit": "ns",
            "range": "± 347.92806952465605"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 114117.21720377605,
            "unit": "ns",
            "range": "± 862.5152769111348"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 151839.58740234375,
            "unit": "ns",
            "range": "± 769.9114916971154"
          }
        ]
      }
    ]
  }
}