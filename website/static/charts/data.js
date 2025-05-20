window.BENCHMARK_DATA = {
  "lastUpdate": 1747776383086,
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
    ]
  }
}