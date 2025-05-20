window.BENCHMARK_DATA = {
  "lastUpdate": 1747775991529,
  "repoUrl": "https://github.com/microsoft/garnet",
  "entries": {
    "Network.BasicOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775587561,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 87.48132560934339,
            "unit": "ns",
            "range": "± 0.27824496025113304"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775609566,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1023.6185567010309,
            "unit": "ns",
            "range": "± 548.4167067435084"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 920.4845360824743,
            "unit": "ns",
            "range": "± 307.38036674866373"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1662.7395833333333,
            "unit": "ns",
            "range": "± 460.7244237363513"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 447685.8333333333,
            "unit": "ns",
            "range": "± 9550.53327892033"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1761.9526315789474,
            "unit": "ns",
            "range": "± 509.5645151232456"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 8287.969072164948,
            "unit": "ns",
            "range": "± 815.681826929781"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1076.458762886598,
            "unit": "ns",
            "range": "± 390.5141354015928"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 828.9278350515464,
            "unit": "ns",
            "range": "± 336.0864300860308"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1828.2680412371135,
            "unit": "ns",
            "range": "± 362.44673503679354"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 441036.42268041236,
            "unit": "ns",
            "range": "± 60739.79381358976"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 1897.5515463917525,
            "unit": "ns",
            "range": "± 370.6494685889787"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 7780.875,
            "unit": "ns",
            "range": "± 158.60049390423305"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 999.4782608695652,
            "unit": "ns",
            "range": "± 294.4017020324094"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 852.5425531914893,
            "unit": "ns",
            "range": "± 314.02307684851934"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1693.5051546391753,
            "unit": "ns",
            "range": "± 408.43640171266105"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 397544.17647058825,
            "unit": "ns",
            "range": "± 7815.736691407392"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 1748.3453608247423,
            "unit": "ns",
            "range": "± 408.2107896103378"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 7943.857142857143,
            "unit": "ns",
            "range": "± 136.93505318657014"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1058.3762886597938,
            "unit": "ns",
            "range": "± 381.0873220616011"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 818.6,
            "unit": "ns",
            "range": "± 353.7964262622718"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1678.9263157894736,
            "unit": "ns",
            "range": "± 323.0531950829514"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 461439.32352941175,
            "unit": "ns",
            "range": "± 20827.503293626083"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1748.2061855670104,
            "unit": "ns",
            "range": "± 413.8890737601162"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 8019.933333333333,
            "unit": "ns",
            "range": "± 132.49876908682512"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1038.0463917525774,
            "unit": "ns",
            "range": "± 463.7569635734719"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 739.1888888888889,
            "unit": "ns",
            "range": "± 333.21646284526685"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 1498.6842105263158,
            "unit": "ns",
            "range": "± 457.5878304985621"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 452699.4736842105,
            "unit": "ns",
            "range": "± 9923.10548595449"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 1894.1030927835052,
            "unit": "ns",
            "range": "± 397.7526839563772"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 7875.1,
            "unit": "ns",
            "range": "± 149.45701723238022"
          }
        ]
      }
    ],
    "Lua.LuaRunnerOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775625453,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3340.505617977528,
            "unit": "ns",
            "range": "± 650.7482777470922"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 4060.75,
            "unit": "ns",
            "range": "± 578.8283661609264"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 366263.51,
            "unit": "ns",
            "range": "± 41582.232305942336"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 422071.1666666667,
            "unit": "ns",
            "range": "± 8666.819066721891"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 15762.266666666666,
            "unit": "ns",
            "range": "± 296.10168780979535"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 131464.1875,
            "unit": "ns",
            "range": "± 1891.9636965773595"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3851.0333333333333,
            "unit": "ns",
            "range": "± 343.45054924294544"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3934.076923076923,
            "unit": "ns",
            "range": "± 470.52647182040755"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 376757.0206185567,
            "unit": "ns",
            "range": "± 52493.88203419902"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 380146.5257731959,
            "unit": "ns",
            "range": "± 54683.17854775147"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 20335.071428571428,
            "unit": "ns",
            "range": "± 202.48284420612578"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 140870.14130434784,
            "unit": "ns",
            "range": "± 14850.91625587978"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3675.426315789474,
            "unit": "ns",
            "range": "± 833.3250016306155"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 3561.1195652173915,
            "unit": "ns",
            "range": "± 416.99226130303924"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 330931.13157894736,
            "unit": "ns",
            "range": "± 11268.675519119008"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 323641.64285714284,
            "unit": "ns",
            "range": "± 5584.199765094683"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 15286.81914893617,
            "unit": "ns",
            "range": "± 2105.328891440658"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 154092.15151515152,
            "unit": "ns",
            "range": "± 26500.382067081573"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3417.9947368421053,
            "unit": "ns",
            "range": "± 535.8616817736468"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3397.121052631579,
            "unit": "ns",
            "range": "± 470.9423820507623"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 440086.6046511628,
            "unit": "ns",
            "range": "± 23839.358350168382"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 431913.1111111111,
            "unit": "ns",
            "range": "± 19646.511356662162"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 19622.076923076922,
            "unit": "ns",
            "range": "± 2551.8704217833333"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 164899.8686868687,
            "unit": "ns",
            "range": "± 31168.20283775826"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3624.223404255319,
            "unit": "ns",
            "range": "± 543.9713614033336"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3487.7708333333335,
            "unit": "ns",
            "range": "± 492.24468624897395"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 427156.3125,
            "unit": "ns",
            "range": "± 16837.98629954515"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 442247.7894736842,
            "unit": "ns",
            "range": "± 14870.034868200808"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 15929.07142857143,
            "unit": "ns",
            "range": "± 267.28484209394816"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 133883.875,
            "unit": "ns",
            "range": "± 1959.5074168440055"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775628849,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 134576.5752034505,
            "unit": "ns",
            "range": "± 900.4118317925404"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 147390.7988106864,
            "unit": "ns",
            "range": "± 646.7753157494541"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 138579.65166766828,
            "unit": "ns",
            "range": "± 326.9461523439644"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775626764,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 18375.589431762695,
            "unit": "ns",
            "range": "± 21.461598324944525"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 18460.35649210612,
            "unit": "ns",
            "range": "± 142.45958371825995"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 19070.9879486084,
            "unit": "ns",
            "range": "± 55.95411013057491"
          }
        ]
      }
    ],
    "Operations.BasicOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775644774,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1744.444366308359,
            "unit": "ns",
            "range": "± 2.006926843541664"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1681.3493701934815,
            "unit": "ns",
            "range": "± 7.0000980469712175"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1677.4785311772273,
            "unit": "ns",
            "range": "± 2.247331343563738"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775645267,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 36613.976623535156,
            "unit": "ns",
            "range": "± 33.96617854298885"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 41738.13788248698,
            "unit": "ns",
            "range": "± 180.8387827606464"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 32097.37685953776,
            "unit": "ns",
            "range": "± 131.87985453167258"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 31987.90769304548,
            "unit": "ns",
            "range": "± 153.87638730474202"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775712970,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15971.150716145834,
            "unit": "ns",
            "range": "± 20.00931650892811"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15751.804645244893,
            "unit": "ns",
            "range": "± 12.49431520863001"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 15691.555786132812,
            "unit": "ns",
            "range": "± 15.402595557459616"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775734659,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 102798.26284555289,
            "unit": "ns",
            "range": "± 200.88232982876397"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 120604.14713541667,
            "unit": "ns",
            "range": "± 679.0471675794456"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 101244.73876953125,
            "unit": "ns",
            "range": "± 193.27886173981238"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (windows-latest  net8.0 Release)": [
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
        "date": 1747775745401,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 35299.539388020836,
            "unit": "ns",
            "range": "± 56.74061634944844"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 37020.73974609375,
            "unit": "ns",
            "range": "± 26.66004471782534"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 31954.986807016227,
            "unit": "ns",
            "range": "± 31.90572444275803"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30556.974574497766,
            "unit": "ns",
            "range": "± 35.51914206066131"
          }
        ]
      }
    ]
  }
}