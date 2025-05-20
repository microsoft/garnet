window.BENCHMARK_DATA = {
  "lastUpdate": 1747778090004,
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
    ],
    "Operations.BasicOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775760359,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1705.1774842398506,
            "unit": "ns",
            "range": "± 2.421500647594731"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1647.641150156657,
            "unit": "ns",
            "range": "± 1.5141922972877275"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1672.6862362452916,
            "unit": "ns",
            "range": "± 2.8585118666081"
          }
        ]
      }
    ],
    "Network.BasicOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775692079,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 82.70690716229953,
            "unit": "ns",
            "range": "± 0.11720350542700275"
          }
        ]
      }
    ],
    "Network.RawStringOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775765385,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 234.8147007737841,
            "unit": "ns",
            "range": "± 0.6467027349973138"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 293.9274127483368,
            "unit": "ns",
            "range": "± 1.3481450770646324"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 310.5818476676941,
            "unit": "ns",
            "range": "± 0.3640044804560835"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 323.19113568464917,
            "unit": "ns",
            "range": "± 0.5740173552219751"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 235.0235345547016,
            "unit": "ns",
            "range": "± 0.5376121053455232"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 177.03230591920706,
            "unit": "ns",
            "range": "± 0.20133411750254251"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 309.7032988230387,
            "unit": "ns",
            "range": "± 1.3028982761687458"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 309.3590372892526,
            "unit": "ns",
            "range": "± 0.19589588809778974"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 365.28500016530353,
            "unit": "ns",
            "range": "± 1.4125594226294262"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 372.24349371592206,
            "unit": "ns",
            "range": "± 1.961664671975273"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775782241,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 18391.779538472492,
            "unit": "ns",
            "range": "± 11.091296176518785"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 17395.40080479213,
            "unit": "ns",
            "range": "± 49.630337462773305"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 15173.510440063477,
            "unit": "ns",
            "range": "± 37.63812456676944"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 14091.132762654623,
            "unit": "ns",
            "range": "± 39.21069791296242"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 112579.82765080378,
            "unit": "ns",
            "range": "± 144.99735649190194"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 20822.452229817707,
            "unit": "ns",
            "range": "± 89.36739591732905"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19643.973515101843,
            "unit": "ns",
            "range": "± 82.14657805755962"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15885.911794809195,
            "unit": "ns",
            "range": "± 7.954396051534785"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 14870.89786936442,
            "unit": "ns",
            "range": "± 51.83018284704106"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 125627.89567057292,
            "unit": "ns",
            "range": "± 1018.5361277671249"
          }
        ]
      }
    ],
    "Operations.CustomOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775802265,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 34176.88912527902,
            "unit": "ns",
            "range": "± 157.35022696127655"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 158966.54910714287,
            "unit": "ns",
            "range": "± 319.91858775978164"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 113627.96634521484,
            "unit": "ns",
            "range": "± 403.350830875267"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 82870.5360952524,
            "unit": "ns",
            "range": "± 169.43059337888315"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 34199.90265401205,
            "unit": "ns",
            "range": "± 33.03491065228018"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 171514.48707932694,
            "unit": "ns",
            "range": "± 672.3791335151013"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 124821.61741129558,
            "unit": "ns",
            "range": "± 398.4573316566253"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 105559.23770577567,
            "unit": "ns",
            "range": "± 583.6439885084577"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 35748.16896158854,
            "unit": "ns",
            "range": "± 219.1234025891164"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 159579.46385091144,
            "unit": "ns",
            "range": "± 713.5882937826702"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 112759.82457682291,
            "unit": "ns",
            "range": "± 390.72853109951666"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 81573.95843098959,
            "unit": "ns",
            "range": "± 416.31369292083946"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775896371,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16360.060628255209,
            "unit": "ns",
            "range": "± 16.72268267794175"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 15097.233072916666,
            "unit": "ns",
            "range": "± 14.73434103802105"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14576.812744140625,
            "unit": "ns",
            "range": "± 10.42895726155623"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13265.641784667969,
            "unit": "ns",
            "range": "± 7.866098849877096"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 117162.06242487981,
            "unit": "ns",
            "range": "± 72.85508800054725"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19787.069484165735,
            "unit": "ns",
            "range": "± 22.676006332711843"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19546.87978108724,
            "unit": "ns",
            "range": "± 46.20842509911649"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15296.588643391928,
            "unit": "ns",
            "range": "± 9.416518593666693"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 13971.952601841518,
            "unit": "ns",
            "range": "± 35.432144857501335"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 116814.26955003005,
            "unit": "ns",
            "range": "± 170.60416949274622"
          }
        ]
      }
    ],
    "Network.RawStringOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775897438,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 228.67209911346436,
            "unit": "ns",
            "range": "± 0.23503511174896163"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 271.4279753821237,
            "unit": "ns",
            "range": "± 0.7053751003957393"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 297.37119334084645,
            "unit": "ns",
            "range": "± 0.6868797539651281"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 297.07326889038086,
            "unit": "ns",
            "range": "± 0.31960060180536326"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 216.42407735188803,
            "unit": "ns",
            "range": "± 0.21428098270295454"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 170.87945938110352,
            "unit": "ns",
            "range": "± 0.23990951837306415"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 319.21072006225586,
            "unit": "ns",
            "range": "± 0.41865933685523166"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 314.78252092997235,
            "unit": "ns",
            "range": "± 0.29861403756097776"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 353.4672663762019,
            "unit": "ns",
            "range": "± 1.2274193979097794"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 357.59474754333496,
            "unit": "ns",
            "range": "± 0.9894952649931728"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775927265,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 836.1702127659574,
            "unit": "ns",
            "range": "± 732.0395817240399"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 969.8979591836735,
            "unit": "ns",
            "range": "± 726.761627844462"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1752.6881720430108,
            "unit": "ns",
            "range": "± 886.1974701240304"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 384502.12765957444,
            "unit": "ns",
            "range": "± 74998.26769142547"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 2145.1612903225805,
            "unit": "ns",
            "range": "± 1027.0807225623435"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 6283.695652173913,
            "unit": "ns",
            "range": "± 1508.8222008663056"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 907.4468085106383,
            "unit": "ns",
            "range": "± 709.1171797098117"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 828.421052631579,
            "unit": "ns",
            "range": "± 515.6411332268558"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1830.5263157894738,
            "unit": "ns",
            "range": "± 622.4648472020242"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 405925,
            "unit": "ns",
            "range": "± 87999.09248269426"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 2765.656565656566,
            "unit": "ns",
            "range": "± 1665.5673554507443"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 6493.023255813953,
            "unit": "ns",
            "range": "± 1063.931742356614"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1085.483870967742,
            "unit": "ns",
            "range": "± 702.5798981781238"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 896.8085106382979,
            "unit": "ns",
            "range": "± 599.5880535044746"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 2632.6530612244896,
            "unit": "ns",
            "range": "± 1845.6287454473231"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 428570.202020202,
            "unit": "ns",
            "range": "± 95482.74978278125"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 3209.375,
            "unit": "ns",
            "range": "± 2571.670688789478"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 10265.30612244898,
            "unit": "ns",
            "range": "± 4576.252057635557"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1125,
            "unit": "ns",
            "range": "± 756.1676227426103"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 798.936170212766,
            "unit": "ns",
            "range": "± 664.3769303335034"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 2142.553191489362,
            "unit": "ns",
            "range": "± 1640.5801073212156"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 480414,
            "unit": "ns",
            "range": "± 102314.14391199511"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 4150,
            "unit": "ns",
            "range": "± 2576.459650352962"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 8380.612244897959,
            "unit": "ns",
            "range": "± 3126.0019643444934"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 972.4489795918367,
            "unit": "ns",
            "range": "± 808.0497856771999"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 726.6666666666666,
            "unit": "ns",
            "range": "± 551.1581269462299"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 2012.7659574468084,
            "unit": "ns",
            "range": "± 1369.5014431170239"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 426751.16279069765,
            "unit": "ns",
            "range": "± 55127.24637372756"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 3066.6666666666665,
            "unit": "ns",
            "range": "± 1990.2825150706408"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 9978.787878787878,
            "unit": "ns",
            "range": "± 3735.7149065083536"
          }
        ]
      }
    ],
    "Lua.LuaRunnerOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775934487,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 7967.0329670329675,
            "unit": "ns",
            "range": "± 1573.1818391085947"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 8547.959183673469,
            "unit": "ns",
            "range": "± 2342.9310039005636"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 373138.7755102041,
            "unit": "ns",
            "range": "± 73459.18936101835"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 374955.0505050505,
            "unit": "ns",
            "range": "± 80045.92734447018"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 32410.43956043956,
            "unit": "ns",
            "range": "± 7843.183581526619"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 145159,
            "unit": "ns",
            "range": "± 29971.923208364205"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 8482.828282828283,
            "unit": "ns",
            "range": "± 2211.751563900523"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 9108.247422680412,
            "unit": "ns",
            "range": "± 2448.029398954255"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 376515.1515151515,
            "unit": "ns",
            "range": "± 65904.79274578263"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 354250,
            "unit": "ns",
            "range": "± 61530.26118290994"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 34215.05376344086,
            "unit": "ns",
            "range": "± 6251.677100839825"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 144398.97959183675,
            "unit": "ns",
            "range": "± 25303.964462208958"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 8633.529411764706,
            "unit": "ns",
            "range": "± 1322.5018731486894"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 9535.051546391753,
            "unit": "ns",
            "range": "± 2145.831289826538"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 412156,
            "unit": "ns",
            "range": "± 73859.24276225787"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 411162,
            "unit": "ns",
            "range": "± 77712.17847177033"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 37361.458333333336,
            "unit": "ns",
            "range": "± 4734.006199085766"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 147628.86597938143,
            "unit": "ns",
            "range": "± 23752.618095884394"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 9480,
            "unit": "ns",
            "range": "± 1706.2214068993808"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 9507.368421052632,
            "unit": "ns",
            "range": "± 1739.5427441045042"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 505379.1208791209,
            "unit": "ns",
            "range": "± 79193.0686030925"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 547632,
            "unit": "ns",
            "range": "± 124851.04327181494"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 41140.625,
            "unit": "ns",
            "range": "± 5801.20818859041"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 155959.59595959596,
            "unit": "ns",
            "range": "± 25524.505708667904"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 9262.5,
            "unit": "ns",
            "range": "± 1692.7243690198602"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 9600,
            "unit": "ns",
            "range": "± 1754.9341339301"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 505743,
            "unit": "ns",
            "range": "± 106276.32199681977"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 467751.6853932584,
            "unit": "ns",
            "range": "± 61280.000282532426"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 44875,
            "unit": "ns",
            "range": "± 5540.691133943332"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 156000,
            "unit": "ns",
            "range": "± 25814.02886070877"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775935647,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 30669.99440612793,
            "unit": "ns",
            "range": "± 100.6054962620933"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 39918.52168782552,
            "unit": "ns",
            "range": "± 308.9902996753339"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 74856.16492919922,
            "unit": "ns",
            "range": "± 462.97807927998804"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 56584.09691074916,
            "unit": "ns",
            "range": "± 216.23368132312945"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 15947.051197932316,
            "unit": "ns",
            "range": "± 17.920840357620765"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 28203.521579996745,
            "unit": "ns",
            "range": "± 164.7384847275908"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 30777.308052571614,
            "unit": "ns",
            "range": "± 156.4236459223657"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 46452.060943603516,
            "unit": "ns",
            "range": "± 103.12625978644799"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 84924.7438307542,
            "unit": "ns",
            "range": "± 180.04089368282638"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 54045.56595720564,
            "unit": "ns",
            "range": "± 86.56810884186639"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15395.754395705004,
            "unit": "ns",
            "range": "± 8.274245899330461"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 35145.16811319987,
            "unit": "ns",
            "range": "± 330.42131012454337"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 32038.85086931501,
            "unit": "ns",
            "range": "± 151.02397446063998"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 40578.76157924107,
            "unit": "ns",
            "range": "± 50.037146296383746"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 75543.15708571214,
            "unit": "ns",
            "range": "± 274.4511756371622"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 56117.589481898714,
            "unit": "ns",
            "range": "± 202.87073137529845"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 15938.064302884615,
            "unit": "ns",
            "range": "± 13.955918433725325"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 29331.24115224985,
            "unit": "ns",
            "range": "± 52.48661133049801"
          }
        ]
      }
    ],
    "Operations.CustomOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747775957301,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 31267.318522135418,
            "unit": "ns",
            "range": "± 52.18720563414489"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 164701.34102957588,
            "unit": "ns",
            "range": "± 700.3959548339752"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 110827.87556966145,
            "unit": "ns",
            "range": "± 152.79197397510387"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 82121.86560997597,
            "unit": "ns",
            "range": "± 86.56565769948942"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 30886.81640625,
            "unit": "ns",
            "range": "± 54.796216104321985"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 169034.0566781851,
            "unit": "ns",
            "range": "± 551.5950456852422"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 117530.88660606972,
            "unit": "ns",
            "range": "± 303.8522379436241"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 108286.93481445312,
            "unit": "ns",
            "range": "± 256.4420405886703"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 30155.32236735026,
            "unit": "ns",
            "range": "± 47.522464540749176"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 157995.2871469351,
            "unit": "ns",
            "range": "± 234.74035790703726"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 105826.06201171875,
            "unit": "ns",
            "range": "± 133.66396623846686"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 83880.16498272236,
            "unit": "ns",
            "range": "± 100.1904682302639"
          }
        ]
      }
    ],
    "Lua.LuaScripts (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775984481,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 305.56086416244506,
            "unit": "ns",
            "range": "± 1.1893304968565246"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 370.0785728772481,
            "unit": "ns",
            "range": "± 1.146035407276267"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 623.0739733832223,
            "unit": "ns",
            "range": "± 1.230767541580893"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 863.314295132955,
            "unit": "ns",
            "range": "± 3.946104727591335"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 284.2435121169457,
            "unit": "ns",
            "range": "± 0.23851729612909336"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 372.13546688216076,
            "unit": "ns",
            "range": "± 0.9298778467142262"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 652.6954531987508,
            "unit": "ns",
            "range": "± 2.4843433312518908"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 861.8616805076599,
            "unit": "ns",
            "range": "± 2.0347343711591273"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 290.70678329467773,
            "unit": "ns",
            "range": "± 1.2167191108270872"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 365.52667204538983,
            "unit": "ns",
            "range": "± 1.0729083299451365"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 666.6096681867327,
            "unit": "ns",
            "range": "± 1.8116736386023704"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 856.6338329950969,
            "unit": "ns",
            "range": "± 2.267772933306014"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 283.7321545283,
            "unit": "ns",
            "range": "± 0.30070313238984775"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 369.11659404209684,
            "unit": "ns",
            "range": "± 0.6980737374811511"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 642.097524579366,
            "unit": "ns",
            "range": "± 2.7560781800722234"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 856.2031699498494,
            "unit": "ns",
            "range": "± 2.013077212416588"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 279.497638629033,
            "unit": "ns",
            "range": "± 0.1583577488667644"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 366.162690671285,
            "unit": "ns",
            "range": "± 1.3088079303485602"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 626.0515473683676,
            "unit": "ns",
            "range": "± 1.7237495036610921"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 846.2184120178223,
            "unit": "ns",
            "range": "± 2.307847429860053"
          }
        ]
      }
    ],
    "Operations.JsonOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747775989136,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 155769.05694110578,
            "unit": "ns",
            "range": "± 861.1809596872815"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 150478.2303091196,
            "unit": "ns",
            "range": "± 451.0899790161698"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: ACL)",
            "value": 169782.77901204428,
            "unit": "ns",
            "range": "± 1135.2629752420578"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: ACL)",
            "value": 709393.231640625,
            "unit": "ns",
            "range": "± 3516.569641693539"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: ACL)",
            "value": 13726.028377278646,
            "unit": "ns",
            "range": "± 66.64081683228254"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: ACL)",
            "value": 718202.0075334822,
            "unit": "ns",
            "range": "± 2845.3939475014067"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: ACL)",
            "value": 8977764.194791667,
            "unit": "ns",
            "range": "± 50863.54272214515"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 148022.09838867188,
            "unit": "ns",
            "range": "± 1140.3827626247837"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 158375.90912737165,
            "unit": "ns",
            "range": "± 631.5838434113263"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: AOF)",
            "value": 164630.5058419364,
            "unit": "ns",
            "range": "± 996.9016611997874"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: AOF)",
            "value": 694495.2221679688,
            "unit": "ns",
            "range": "± 2551.3766797731414"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: AOF)",
            "value": 14005.843766348702,
            "unit": "ns",
            "range": "± 46.89981492809794"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: AOF)",
            "value": 737024.9216796875,
            "unit": "ns",
            "range": "± 3208.7085469142767"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: AOF)",
            "value": 8861189.097916666,
            "unit": "ns",
            "range": "± 83471.61890181198"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: None)",
            "value": 149157.7610188802,
            "unit": "ns",
            "range": "± 978.1315576246769"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: None)",
            "value": 153759.9044189453,
            "unit": "ns",
            "range": "± 860.353434200776"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: None)",
            "value": 170881.0893717448,
            "unit": "ns",
            "range": "± 1081.1690901483116"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: None)",
            "value": 695754.4956380208,
            "unit": "ns",
            "range": "± 3008.516114464107"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: None)",
            "value": 13553.613356370191,
            "unit": "ns",
            "range": "± 28.260101639620476"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: None)",
            "value": 713297.1121651785,
            "unit": "ns",
            "range": "± 2622.6124875149785"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: None)",
            "value": 8913141.88125,
            "unit": "ns",
            "range": "± 58188.22970155399"
          }
        ]
      }
    ],
    "Lua.LuaScripts (windows-latest  net8.0 Release)": [
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
        "date": 1747776070775,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 157.93283462524414,
            "unit": "ns",
            "range": "± 0.1403083988755362"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 190.5050209590367,
            "unit": "ns",
            "range": "± 0.5925520085115705"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 325.59289591653004,
            "unit": "ns",
            "range": "± 0.5473581142718333"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 358.32314173380536,
            "unit": "ns",
            "range": "± 0.42580410723658113"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 159.655185846182,
            "unit": "ns",
            "range": "± 0.1983684314897916"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 199.15109361921037,
            "unit": "ns",
            "range": "± 0.5636809177334202"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 330.08366266886395,
            "unit": "ns",
            "range": "± 0.5273899446520681"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 360.28179781777516,
            "unit": "ns",
            "range": "± 2.0044365701416105"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 161.64618730545044,
            "unit": "ns",
            "range": "± 0.21134965645795284"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 201.96213126182556,
            "unit": "ns",
            "range": "± 0.28121302927054176"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 304.8746649424235,
            "unit": "ns",
            "range": "± 1.6733594347401906"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 359.8788628211388,
            "unit": "ns",
            "range": "± 0.20440921141433144"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 156.0970581494845,
            "unit": "ns",
            "range": "± 0.15227331385882786"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 189.1476313273112,
            "unit": "ns",
            "range": "± 0.36949967062924216"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 311.7269770304362,
            "unit": "ns",
            "range": "± 0.5992337752381989"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 362.34829743703204,
            "unit": "ns",
            "range": "± 1.0466812790056932"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 162.00763157435827,
            "unit": "ns",
            "range": "± 0.4971156132455223"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 196.28130472623386,
            "unit": "ns",
            "range": "± 0.3389319073903196"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 327.1879768371582,
            "unit": "ns",
            "range": "± 0.43613645667685524"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 369.83422551836287,
            "unit": "ns",
            "range": "± 0.6062912919230198"
          }
        ]
      }
    ],
    "Operations.JsonOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747776078151,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 155884.0909830729,
            "unit": "ns",
            "range": "± 310.76974689076724"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 140603.46842447916,
            "unit": "ns",
            "range": "± 515.3621648945899"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: ACL)",
            "value": 165939.8624674479,
            "unit": "ns",
            "range": "± 667.8727349430003"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: ACL)",
            "value": 497408.30078125,
            "unit": "ns",
            "range": "± 784.7884650013622"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: ACL)",
            "value": 9591.872992882361,
            "unit": "ns",
            "range": "± 11.04213553214794"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: ACL)",
            "value": 512573.7234933036,
            "unit": "ns",
            "range": "± 1296.201273181102"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: ACL)",
            "value": 8764419.754464285,
            "unit": "ns",
            "range": "± 18505.235122547994"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 156579.3017578125,
            "unit": "ns",
            "range": "± 308.95316775362227"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 152560.38724459134,
            "unit": "ns",
            "range": "± 576.8428052725909"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: AOF)",
            "value": 163075.6591796875,
            "unit": "ns",
            "range": "± 568.3069149353753"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: AOF)",
            "value": 491403.52957589284,
            "unit": "ns",
            "range": "± 1785.164670944303"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: AOF)",
            "value": 9704.042663574219,
            "unit": "ns",
            "range": "± 38.28990601735101"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: AOF)",
            "value": 501919.83816964284,
            "unit": "ns",
            "range": "± 664.2611299031574"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: AOF)",
            "value": 8742521.651785715,
            "unit": "ns",
            "range": "± 38245.896571903904"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetCommand(Params: None)",
            "value": 152183.23277064733,
            "unit": "ns",
            "range": "± 345.00988307517576"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonSetCommand(Params: None)",
            "value": 155838.4486607143,
            "unit": "ns",
            "range": "± 436.9636707242007"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetDeepPath(Params: None)",
            "value": 169203.583984375,
            "unit": "ns",
            "range": "± 365.2696424553117"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayPath(Params: None)",
            "value": 489723.5579427083,
            "unit": "ns",
            "range": "± 825.7659266982209"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetArrayElementsPath(Params: None)",
            "value": 9581.264822823661,
            "unit": "ns",
            "range": "± 22.966622961293982"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetFilterPath(Params: None)",
            "value": 511216.69921875,
            "unit": "ns",
            "range": "± 513.9517358491246"
          },
          {
            "name": "BDN.benchmark.Operations.JsonOperations.ModuleJsonGetRecursive(Params: None)",
            "value": 9192773.77232143,
            "unit": "ns",
            "range": "± 20307.27059329449"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747776083541,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 33302.406529017855,
            "unit": "ns",
            "range": "± 37.05070438198154"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 47902.261555989586,
            "unit": "ns",
            "range": "± 40.18334572357876"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 70401.52913411458,
            "unit": "ns",
            "range": "± 161.87488460343786"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 55577.23592122396,
            "unit": "ns",
            "range": "± 34.8142596253256"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 17435.231454031808,
            "unit": "ns",
            "range": "± 17.247888951047937"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 27198.91545222356,
            "unit": "ns",
            "range": "± 22.68505233762802"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 32462.856038411457,
            "unit": "ns",
            "range": "± 40.744817364549874"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 56495.475260416664,
            "unit": "ns",
            "range": "± 119.70778926089547"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 78611.41793387277,
            "unit": "ns",
            "range": "± 146.8607054946065"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 55352.57283528646,
            "unit": "ns",
            "range": "± 94.10397961305652"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 17120.870317731584,
            "unit": "ns",
            "range": "± 24.787819532741555"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 32333.79908970424,
            "unit": "ns",
            "range": "± 58.147334043468014"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 32069.925798688615,
            "unit": "ns",
            "range": "± 25.250772350297858"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 50060.15085073618,
            "unit": "ns",
            "range": "± 38.40043970291042"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 70568.54370117188,
            "unit": "ns",
            "range": "± 172.76420656820412"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 53326.54744466146,
            "unit": "ns",
            "range": "± 83.05720643192288"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 18703.61094156901,
            "unit": "ns",
            "range": "± 80.53709163554453"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 28760.21554129464,
            "unit": "ns",
            "range": "± 173.21920791433303"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747776173618,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 15109.234574538012,
            "unit": "ns",
            "range": "± 39.39705541046069"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 19951.577509562176,
            "unit": "ns",
            "range": "± 14.778215513805389"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 22209.053611246745,
            "unit": "ns",
            "range": "± 116.23801568710496"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 22710.94617789132,
            "unit": "ns",
            "range": "± 85.24134425010652"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 16190.719765799386,
            "unit": "ns",
            "range": "± 29.397523243851587"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 11450.449077061245,
            "unit": "ns",
            "range": "± 28.037761461676656"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 22839.332574026925,
            "unit": "ns",
            "range": "± 100.11533403910022"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 22324.299794123723,
            "unit": "ns",
            "range": "± 22.680652959041545"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 26446.879989624023,
            "unit": "ns",
            "range": "± 70.2473799543706"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 27129.77835998535,
            "unit": "ns",
            "range": "± 111.83289297370254"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 22549.85192667643,
            "unit": "ns",
            "range": "± 93.06636609262681"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26765.289991106307,
            "unit": "ns",
            "range": "± 138.32874091569894"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 28705.61536516462,
            "unit": "ns",
            "range": "± 78.59962585978455"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 30115.154791259767,
            "unit": "ns",
            "range": "± 115.27839558761093"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 16115.062268575033,
            "unit": "ns",
            "range": "± 20.42859827706711"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10389.067210606167,
            "unit": "ns",
            "range": "± 29.4836895178965"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 29011.35013631185,
            "unit": "ns",
            "range": "± 79.58865877310919"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 30045.305525716147,
            "unit": "ns",
            "range": "± 214.19533461172102"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 33174.028067452564,
            "unit": "ns",
            "range": "± 88.98548352716422"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 33890.177010672436,
            "unit": "ns",
            "range": "± 116.07584202917957"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 15264.340051269532,
            "unit": "ns",
            "range": "± 81.77126479979377"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19281.877455647787,
            "unit": "ns",
            "range": "± 86.72331871017182"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 21120.343248639787,
            "unit": "ns",
            "range": "± 99.89739381135723"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 21986.805099487305,
            "unit": "ns",
            "range": "± 88.00893038846459"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 16156.202756754557,
            "unit": "ns",
            "range": "± 95.62596568356868"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10352.390302217924,
            "unit": "ns",
            "range": "± 7.643597466317051"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 23567.616307185246,
            "unit": "ns",
            "range": "± 15.389143798226103"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 22585.531154926008,
            "unit": "ns",
            "range": "± 90.59022171511103"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 26427.157741001673,
            "unit": "ns",
            "range": "± 65.15500712091173"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 27774.341175624304,
            "unit": "ns",
            "range": "± 74.8514798574072"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747776332511,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 158377.5760498047,
            "unit": "ns",
            "range": "± 355.50474583783847"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 18690.93992614746,
            "unit": "ns",
            "range": "± 10.83383970161914"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 16470.1812046596,
            "unit": "ns",
            "range": "± 36.54584759083768"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 155528.61169433594,
            "unit": "ns",
            "range": "± 666.4160053432377"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 46289.07518107096,
            "unit": "ns",
            "range": "± 280.935976587016"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 136855.72981770834,
            "unit": "ns",
            "range": "± 155.46693247929775"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 10541188.586458333,
            "unit": "ns",
            "range": "± 155931.1927883136"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 286313.5150492654,
            "unit": "ns",
            "range": "± 13572.226227105955"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 153688.3437325614,
            "unit": "ns",
            "range": "± 320.2558023079896"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 18981.045031738282,
            "unit": "ns",
            "range": "± 104.87558039126823"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 16561.90967305501,
            "unit": "ns",
            "range": "± 13.407935554757417"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 159494.65895182293,
            "unit": "ns",
            "range": "± 753.09098006833"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 45334.78730656551,
            "unit": "ns",
            "range": "± 107.89196205469148"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 133652.40204729352,
            "unit": "ns",
            "range": "± 271.9681857123164"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 10510391.160590278,
            "unit": "ns",
            "range": "± 211550.7921735041"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 287444.0518294271,
            "unit": "ns",
            "range": "± 14502.228795568832"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 153516.691021259,
            "unit": "ns",
            "range": "± 258.0858826552377"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 19027.309880183293,
            "unit": "ns",
            "range": "± 72.93683545724897"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 16566.992826021633,
            "unit": "ns",
            "range": "± 13.504999414666381"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 154139.74012169472,
            "unit": "ns",
            "range": "± 278.5935654391694"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 45763.69787597656,
            "unit": "ns",
            "range": "± 91.09360605438445"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 134683.82777622767,
            "unit": "ns",
            "range": "± 414.88551556654954"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 8721230.640625,
            "unit": "ns",
            "range": "± 46738.466958225334"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 263462.0407714844,
            "unit": "ns",
            "range": "± 888.4694059084475"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 153373.96616908483,
            "unit": "ns",
            "range": "± 609.6667190181142"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 19491.95537923177,
            "unit": "ns",
            "range": "± 108.6784575803279"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 16526.156936645508,
            "unit": "ns",
            "range": "± 11.524821153380435"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 157671.94705904447,
            "unit": "ns",
            "range": "± 355.98545854896156"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 46511.25348336356,
            "unit": "ns",
            "range": "± 42.55801963990217"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 138406.8622295673,
            "unit": "ns",
            "range": "± 156.1335135600405"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 9489721.138221154,
            "unit": "ns",
            "range": "± 39681.77003705552"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 290910.22237723216,
            "unit": "ns",
            "range": "± 1524.7919079445146"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 154714.07823768028,
            "unit": "ns",
            "range": "± 285.7511553062308"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 19507.443860880532,
            "unit": "ns",
            "range": "± 89.52682610537342"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 16446.847149188703,
            "unit": "ns",
            "range": "± 11.607322538625587"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 155928.43517252605,
            "unit": "ns",
            "range": "± 402.8831165229699"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 48628.04554748535,
            "unit": "ns",
            "range": "± 35.28871825634651"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 132850.49714006696,
            "unit": "ns",
            "range": "± 167.3962836244667"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 9443363.276785715,
            "unit": "ns",
            "range": "± 33915.46530006847"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 285234.7711704799,
            "unit": "ns",
            "range": "± 790.739922957281"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747776359576,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 15403.542218889508,
            "unit": "ns",
            "range": "± 27.46859015633376"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 20317.110392252605,
            "unit": "ns",
            "range": "± 32.84051490417265"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 20822.366550990515,
            "unit": "ns",
            "range": "± 40.51874095470686"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 21882.523404634914,
            "unit": "ns",
            "range": "± 30.474251270712593"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15486.928013392857,
            "unit": "ns",
            "range": "± 49.31192024255529"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 10584.912872314453,
            "unit": "ns",
            "range": "± 20.43638915493138"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 22950.432913643974,
            "unit": "ns",
            "range": "± 46.571817646455145"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 21361.090087890625,
            "unit": "ns",
            "range": "± 19.895547520087245"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 26336.545889718192,
            "unit": "ns",
            "range": "± 58.40094835318555"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 28343.821598933293,
            "unit": "ns",
            "range": "± 58.54330745492679"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 20129.188101632255,
            "unit": "ns",
            "range": "± 102.7556275834707"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26563.316752115887,
            "unit": "ns",
            "range": "± 70.77412038356172"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 26108.082275390625,
            "unit": "ns",
            "range": "± 41.236203686662265"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 27630.5717976888,
            "unit": "ns",
            "range": "± 50.02757640677393"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15620.264107840401,
            "unit": "ns",
            "range": "± 29.902465917072526"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10791.29151564378,
            "unit": "ns",
            "range": "± 15.862087557753224"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 25811.39373779297,
            "unit": "ns",
            "range": "± 40.757964926852964"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 27289.36543782552,
            "unit": "ns",
            "range": "± 35.15338404392607"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 31989.258219401043,
            "unit": "ns",
            "range": "± 95.61522816754294"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 30641.100260416668,
            "unit": "ns",
            "range": "± 139.24908943857753"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14012.530165452223,
            "unit": "ns",
            "range": "± 12.62665134754648"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 20063.995713454027,
            "unit": "ns",
            "range": "± 47.92840912246867"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 20196.87998453776,
            "unit": "ns",
            "range": "± 122.22290105935929"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 20956.932067871094,
            "unit": "ns",
            "range": "± 68.87431691367615"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15269.912250225361,
            "unit": "ns",
            "range": "± 17.057573497825402"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10954.441397530692,
            "unit": "ns",
            "range": "± 18.563181118722945"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 22504.34112548828,
            "unit": "ns",
            "range": "± 60.985683861353294"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 22612.444716233473,
            "unit": "ns",
            "range": "± 14.86339025474496"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 27777.9052734375,
            "unit": "ns",
            "range": "± 38.61041837301868"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 26906.729561941964,
            "unit": "ns",
            "range": "± 32.73222796671953"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747776547827,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 136510.69400227864,
            "unit": "ns",
            "range": "± 761.4649845592462"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 11339.127879551479,
            "unit": "ns",
            "range": "± 42.40449335346187"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11470.403257751464,
            "unit": "ns",
            "range": "± 64.57785390568435"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10250.261864798409,
            "unit": "ns",
            "range": "± 49.49833255897645"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 12676.702328273228,
            "unit": "ns",
            "range": "± 33.87921534694891"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 13419.354404703776,
            "unit": "ns",
            "range": "± 41.308625498070896"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 12324.768129621234,
            "unit": "ns",
            "range": "± 4.5002598860034535"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10235.833882258488,
            "unit": "ns",
            "range": "± 28.907431691517544"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 12771.909962972006,
            "unit": "ns",
            "range": "± 51.40764881005062"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13228.28394141564,
            "unit": "ns",
            "range": "± 37.75574094310592"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 12695.036703745523,
            "unit": "ns",
            "range": "± 5.7756566934464235"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 13236.007313028971,
            "unit": "ns",
            "range": "± 34.988430028159534"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12841.275242396763,
            "unit": "ns",
            "range": "± 42.132885298415225"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 12042.097306387765,
            "unit": "ns",
            "range": "± 45.558278901595614"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 13028.709436269906,
            "unit": "ns",
            "range": "± 26.730566982774803"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 150747.06088053386,
            "unit": "ns",
            "range": "± 462.9115754464225"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 45029.918005371095,
            "unit": "ns",
            "range": "± 161.54626451914518"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 48216.6575764974,
            "unit": "ns",
            "range": "± 156.83121318629784"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 52472.81185913086,
            "unit": "ns",
            "range": "± 116.66647999259838"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 81794.51337890625,
            "unit": "ns",
            "range": "± 286.52323555285267"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 112350.601335798,
            "unit": "ns",
            "range": "± 254.56170227226997"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 48968.383479817705,
            "unit": "ns",
            "range": "± 147.793661447527"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 46949.61478969029,
            "unit": "ns",
            "range": "± 76.7871491100124"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 54787.859627859936,
            "unit": "ns",
            "range": "± 185.87037053121918"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 88060.36042480469,
            "unit": "ns",
            "range": "± 366.4867784358226"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 62525.93756573017,
            "unit": "ns",
            "range": "± 99.76848995726833"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 13382.051889546712,
            "unit": "ns",
            "range": "± 25.793937858839215"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 78814.18649088542,
            "unit": "ns",
            "range": "± 663.4358660187751"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 46649.47876412528,
            "unit": "ns",
            "range": "± 118.30925820923905"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 48673.00828669621,
            "unit": "ns",
            "range": "± 224.7135176498997"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 139417.39981515068,
            "unit": "ns",
            "range": "± 481.8599548023745"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 44557.741998291014,
            "unit": "ns",
            "range": "± 261.15682265282805"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 48028.143231201175,
            "unit": "ns",
            "range": "± 174.69518347270002"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 49849.26688842774,
            "unit": "ns",
            "range": "± 120.73492072870138"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 75218.11960274832,
            "unit": "ns",
            "range": "± 362.6302093792746"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 102940.08916364398,
            "unit": "ns",
            "range": "± 198.9454078590874"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 48557.341478620256,
            "unit": "ns",
            "range": "± 77.4615928386349"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 40299.562856820914,
            "unit": "ns",
            "range": "± 116.52605254224048"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 54778.2677961077,
            "unit": "ns",
            "range": "± 177.39025899208673"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 77188.84478352865,
            "unit": "ns",
            "range": "± 255.9591312914279"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 60107.740631103516,
            "unit": "ns",
            "range": "± 167.1597816171405"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 13335.995068868002,
            "unit": "ns",
            "range": "± 21.826802590929365"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 76532.95798903245,
            "unit": "ns",
            "range": "± 208.33731456580983"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 49005.065368652344,
            "unit": "ns",
            "range": "± 141.4807675084981"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 52544.21294759114,
            "unit": "ns",
            "range": "± 94.43096599287341"
          }
        ]
      }
    ],
    "Operations.SetOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747776644473,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 160280.56311848958,
            "unit": "ns",
            "range": "± 1218.3828455561986"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 80344.08605957031,
            "unit": "ns",
            "range": "± 205.75588767399032"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10160.413874308268,
            "unit": "ns",
            "range": "± 4.672579129392403"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 12183.777072143555,
            "unit": "ns",
            "range": "± 60.81564502993411"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 26027.53347269694,
            "unit": "ns",
            "range": "± 32.82374871640471"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 12120.365942891438,
            "unit": "ns",
            "range": "± 36.949986351798294"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 14676.57245686849,
            "unit": "ns",
            "range": "± 51.16900117064513"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 12865.735218593052,
            "unit": "ns",
            "range": "± 53.33001790800075"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 11245.081647600446,
            "unit": "ns",
            "range": "± 60.342916188316494"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 12551.423882039388,
            "unit": "ns",
            "range": "± 48.311418346741185"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 13835.564008585612,
            "unit": "ns",
            "range": "± 48.34664393664629"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 12770.677053833007,
            "unit": "ns",
            "range": "± 45.5334010481186"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 14852.752277919224,
            "unit": "ns",
            "range": "± 68.78315133778312"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 14259.104545006385,
            "unit": "ns",
            "range": "± 13.90179226905838"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 12583.84171702067,
            "unit": "ns",
            "range": "± 52.25423999759851"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 14350.90642344157,
            "unit": "ns",
            "range": "± 54.22500090378766"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 174872.89587402344,
            "unit": "ns",
            "range": "± 610.6150292079036"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 192164.4595424107,
            "unit": "ns",
            "range": "± 1482.4550118238406"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 45860.523616536455,
            "unit": "ns",
            "range": "± 210.31609202403826"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 47546.99587809245,
            "unit": "ns",
            "range": "± 204.18959219752728"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 244340.92972005208,
            "unit": "ns",
            "range": "± 1828.317863082375"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 66332.61826578777,
            "unit": "ns",
            "range": "± 346.11747786617923"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 69242.84086390903,
            "unit": "ns",
            "range": "± 167.89221151285267"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 56485.29375406901,
            "unit": "ns",
            "range": "± 262.60701135388774"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 62993.122017996655,
            "unit": "ns",
            "range": "± 173.73363909948193"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 164142.33948567708,
            "unit": "ns",
            "range": "± 827.1231638836465"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 257813.86683872767,
            "unit": "ns",
            "range": "± 1492.1790054201722"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 161928.05699869792,
            "unit": "ns",
            "range": "± 691.201070897523"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 241237.02974446616,
            "unit": "ns",
            "range": "± 1581.137468800067"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 152464.2236409505,
            "unit": "ns",
            "range": "± 886.9180987744518"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 161801.87626953126,
            "unit": "ns",
            "range": "± 608.132517841112"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 239358.16764322916,
            "unit": "ns",
            "range": "± 1207.5709942562796"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 162904.70056152344,
            "unit": "ns",
            "range": "± 575.6070837531362"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 180002.66357421875,
            "unit": "ns",
            "range": "± 459.86568296892864"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 40431.389864408055,
            "unit": "ns",
            "range": "± 62.17939168805018"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 55416.58341064453,
            "unit": "ns",
            "range": "± 118.59839316828062"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 239131.90652901787,
            "unit": "ns",
            "range": "± 886.5704546017786"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 60478.949527994795,
            "unit": "ns",
            "range": "± 131.35317571636406"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 67236.53813476562,
            "unit": "ns",
            "range": "± 430.7784580999057"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 59291.86428222656,
            "unit": "ns",
            "range": "± 138.36274629552014"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 65087.61844482422,
            "unit": "ns",
            "range": "± 349.7621950226696"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 159142.26341145832,
            "unit": "ns",
            "range": "± 1073.5057659782497"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 207361.08299153644,
            "unit": "ns",
            "range": "± 1198.696589060388"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 142106.45409217247,
            "unit": "ns",
            "range": "± 432.9782140954748"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 211330.17770620494,
            "unit": "ns",
            "range": "± 344.7454017248322"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 142387.13553059896,
            "unit": "ns",
            "range": "± 887.652781655836"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 147411.79346516926,
            "unit": "ns",
            "range": "± 580.6441307369269"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 208892.9292480469,
            "unit": "ns",
            "range": "± 1026.977687480315"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747776691051,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 91945.1835123698,
            "unit": "ns",
            "range": "± 337.5990859494983"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 26317.73423414964,
            "unit": "ns",
            "range": "± 15.871511274753919"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 23577.508646647137,
            "unit": "ns",
            "range": "± 38.83605808669586"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 76208.40105329241,
            "unit": "ns",
            "range": "± 60.22190036342823"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 31258.093668619793,
            "unit": "ns",
            "range": "± 41.16272591923894"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 82806.19245256696,
            "unit": "ns",
            "range": "± 460.0600419456193"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 5481549.944196428,
            "unit": "ns",
            "range": "± 87408.17554188523"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 154264.09594726562,
            "unit": "ns",
            "range": "± 13209.472624947488"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 90752.60498046875,
            "unit": "ns",
            "range": "± 249.0558836414199"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 25946.783447265625,
            "unit": "ns",
            "range": "± 22.978167966008268"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 23596.831923264723,
            "unit": "ns",
            "range": "± 28.38075765833158"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 77435.95815805289,
            "unit": "ns",
            "range": "± 84.18254823604912"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 31163.712419782365,
            "unit": "ns",
            "range": "± 47.19806402147927"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 74639.84723772321,
            "unit": "ns",
            "range": "± 128.62814787790393"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 5575514.84375,
            "unit": "ns",
            "range": "± 97257.68080683386"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 155600.81616210938,
            "unit": "ns",
            "range": "± 13041.992405278945"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 91064.9755859375,
            "unit": "ns",
            "range": "± 393.9494667723081"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 25848.106384277344,
            "unit": "ns",
            "range": "± 19.36898649038986"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 23539.615304129464,
            "unit": "ns",
            "range": "± 11.17474183983661"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 75511.39200846355,
            "unit": "ns",
            "range": "± 127.4805673370106"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 31694.000244140625,
            "unit": "ns",
            "range": "± 49.17930335484913"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 74951.25034877232,
            "unit": "ns",
            "range": "± 117.54104683424254"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 4520531.705729167,
            "unit": "ns",
            "range": "± 11604.376597878965"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 140702.12890625,
            "unit": "ns",
            "range": "± 184.3871692677816"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 92611.2531389509,
            "unit": "ns",
            "range": "± 214.78691867859564"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 25476.51885986328,
            "unit": "ns",
            "range": "± 15.7682477168231"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 23493.469020298548,
            "unit": "ns",
            "range": "± 12.417432703675097"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 77283.06274414062,
            "unit": "ns",
            "range": "± 81.0806783911991"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 31199.153238932293,
            "unit": "ns",
            "range": "± 38.29506400093218"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 76819.16463216145,
            "unit": "ns",
            "range": "± 88.9843738931812"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 5104936.9140625,
            "unit": "ns",
            "range": "± 10988.333582112278"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 169263.33821614584,
            "unit": "ns",
            "range": "± 255.0362184169795"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 90293.84329659598,
            "unit": "ns",
            "range": "± 155.42488884746987"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 25730.055745442707,
            "unit": "ns",
            "range": "± 13.33983949061309"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 24616.224670410156,
            "unit": "ns",
            "range": "± 14.422891059504742"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 76867.8389485677,
            "unit": "ns",
            "range": "± 207.0053952191138"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 31385.350748697918,
            "unit": "ns",
            "range": "± 45.885368032395135"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 76683.06448800223,
            "unit": "ns",
            "range": "± 182.31178500672013"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 5168693.269230769,
            "unit": "ns",
            "range": "± 9363.890610269336"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 157169.08447265625,
            "unit": "ns",
            "range": "± 283.32321465145606"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747776800025,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 106584.88850911458,
            "unit": "ns",
            "range": "± 172.0116716231253"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 12387.698473249164,
            "unit": "ns",
            "range": "± 20.619174541816793"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 12163.106536865234,
            "unit": "ns",
            "range": "± 6.2571357557770195"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10444.493212018695,
            "unit": "ns",
            "range": "± 24.669856825936105"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 14836.940648005559,
            "unit": "ns",
            "range": "± 15.995182551961125"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 16744.827270507812,
            "unit": "ns",
            "range": "± 26.49818820915569"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 14501.201985677084,
            "unit": "ns",
            "range": "± 7.838171913294944"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10323.776652018229,
            "unit": "ns",
            "range": "± 75.23024962596942"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 14944.57982381185,
            "unit": "ns",
            "range": "± 8.437588293780031"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 12893.583679199219,
            "unit": "ns",
            "range": "± 12.081961815383426"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 14533.661106654576,
            "unit": "ns",
            "range": "± 26.509394153057567"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 9376.361628941127,
            "unit": "ns",
            "range": "± 13.456591716922269"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 14062.06293741862,
            "unit": "ns",
            "range": "± 18.512720428496255"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 15610.978111853967,
            "unit": "ns",
            "range": "± 12.17918012321407"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 15459.72160926232,
            "unit": "ns",
            "range": "± 10.723212162106565"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 116634.86653645833,
            "unit": "ns",
            "range": "± 302.64056530311296"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 43588.853352864586,
            "unit": "ns",
            "range": "± 60.65783149551434"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 44413.43340192522,
            "unit": "ns",
            "range": "± 95.08260966131067"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 46586.73828125,
            "unit": "ns",
            "range": "± 92.68460835952156"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 69530.2518717448,
            "unit": "ns",
            "range": "± 201.51556406723134"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 96692.72054036458,
            "unit": "ns",
            "range": "± 248.85750873901966"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 47040.95438639323,
            "unit": "ns",
            "range": "± 93.36794986172924"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 37646.42216609074,
            "unit": "ns",
            "range": "± 41.961174859814555"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 47615.78837076823,
            "unit": "ns",
            "range": "± 68.46792545499065"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 67419.50520833333,
            "unit": "ns",
            "range": "± 279.8689838281762"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 56232.81555175781,
            "unit": "ns",
            "range": "± 111.3319675719354"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 9432.826056847205,
            "unit": "ns",
            "range": "± 10.61472195823405"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 56481.527506510414,
            "unit": "ns",
            "range": "± 117.19680308396471"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 45360.84115164621,
            "unit": "ns",
            "range": "± 120.12036612013053"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 46981.06241861979,
            "unit": "ns",
            "range": "± 88.6951954962089"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 106503.05419921875,
            "unit": "ns",
            "range": "± 370.02738743803206"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 43741.81620279948,
            "unit": "ns",
            "range": "± 100.75104070751307"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 42767.6513671875,
            "unit": "ns",
            "range": "± 70.65136696411264"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 46188.72331891741,
            "unit": "ns",
            "range": "± 63.727462075694326"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 61797.29352678572,
            "unit": "ns",
            "range": "± 137.71652509293781"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 87238.04844447544,
            "unit": "ns",
            "range": "± 163.96892502256125"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 48453.346470424105,
            "unit": "ns",
            "range": "± 48.87984863446056"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 36586.08927408854,
            "unit": "ns",
            "range": "± 61.00462370496355"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 47988.67710658482,
            "unit": "ns",
            "range": "± 65.76863875131785"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 63441.99741908482,
            "unit": "ns",
            "range": "± 140.74671659516687"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 56006.7380464994,
            "unit": "ns",
            "range": "± 59.543235837905954"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 9409.85379900251,
            "unit": "ns",
            "range": "± 17.504682788155954"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 52063.77970377604,
            "unit": "ns",
            "range": "± 91.81351214283193"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 47187.08932059152,
            "unit": "ns",
            "range": "± 60.27004211976834"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 49483.79647391183,
            "unit": "ns",
            "range": "± 72.19194889287292"
          }
        ]
      }
    ],
    "Operations.SetOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747776895507,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 126223.86474609375,
            "unit": "ns",
            "range": "± 711.8836377441678"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 67683.61554827009,
            "unit": "ns",
            "range": "± 167.19628482905105"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10546.161433628627,
            "unit": "ns",
            "range": "± 7.962008461551787"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 13942.857033865792,
            "unit": "ns",
            "range": "± 25.200713944194938"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 29420.740400041854,
            "unit": "ns",
            "range": "± 28.49491310264699"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 15754.044165978064,
            "unit": "ns",
            "range": "± 13.303225780010841"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 20253.24493408203,
            "unit": "ns",
            "range": "± 20.65438097595593"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 16894.010823567707,
            "unit": "ns",
            "range": "± 58.98801608142129"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 12410.332958514873,
            "unit": "ns",
            "range": "± 9.932232272749044"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 16206.450500488281,
            "unit": "ns",
            "range": "± 25.0896104391678"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 20311.28117487981,
            "unit": "ns",
            "range": "± 25.45575239483903"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 17138.289466271035,
            "unit": "ns",
            "range": "± 19.22716855702406"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 21348.67645263672,
            "unit": "ns",
            "range": "± 21.52831637828022"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 20907.942810058594,
            "unit": "ns",
            "range": "± 20.865360339411655"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 16861.715044294084,
            "unit": "ns",
            "range": "± 15.333647852004434"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 18321.42598470052,
            "unit": "ns",
            "range": "± 11.790614588745662"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 131786.62632533483,
            "unit": "ns",
            "range": "± 411.11772118067097"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 145502.7205403646,
            "unit": "ns",
            "range": "± 959.8498163256082"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 35983.47920735677,
            "unit": "ns",
            "range": "± 61.474565807300976"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 45572.96055385045,
            "unit": "ns",
            "range": "± 59.71806757661554"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 225054.58809988838,
            "unit": "ns",
            "range": "± 385.8438959203107"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 46689.82421875,
            "unit": "ns",
            "range": "± 121.73033692547958"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 55330.65572102865,
            "unit": "ns",
            "range": "± 93.8256779371996"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 56568.23872884115,
            "unit": "ns",
            "range": "± 93.59115473674966"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 57657.850748697914,
            "unit": "ns",
            "range": "± 121.73641950303367"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 142866.32080078125,
            "unit": "ns",
            "range": "± 779.8536586112878"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 228718.88997395834,
            "unit": "ns",
            "range": "± 619.175668094613"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 140711.6316731771,
            "unit": "ns",
            "range": "± 838.903429016665"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 219847.39583333334,
            "unit": "ns",
            "range": "± 587.7390646624114"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 139677.2355143229,
            "unit": "ns",
            "range": "± 1181.2596845650326"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 145464.345703125,
            "unit": "ns",
            "range": "± 693.665280121638"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 213419.39697265625,
            "unit": "ns",
            "range": "± 535.1675856963915"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 126934.62960379464,
            "unit": "ns",
            "range": "± 392.2142469156226"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 138303.13546316963,
            "unit": "ns",
            "range": "± 457.5755792168681"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 36805.592041015625,
            "unit": "ns",
            "range": "± 81.48435766248384"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 46030.30517578125,
            "unit": "ns",
            "range": "± 50.9039006136429"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 213678.6971028646,
            "unit": "ns",
            "range": "± 395.3559741155822"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 45796.26688639323,
            "unit": "ns",
            "range": "± 57.349801122299034"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 56993.956705729164,
            "unit": "ns",
            "range": "± 86.93870809969069"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 55988.18359375,
            "unit": "ns",
            "range": "± 90.18413774872494"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 56839.814453125,
            "unit": "ns",
            "range": "± 84.35813288654505"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 144974.5849609375,
            "unit": "ns",
            "range": "± 372.47905351368547"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 204545.7972935268,
            "unit": "ns",
            "range": "± 443.811096029299"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 127054.28185096153,
            "unit": "ns",
            "range": "± 153.01608784120452"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 175956.8150111607,
            "unit": "ns",
            "range": "± 510.36802870046614"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 130636.32463727679,
            "unit": "ns",
            "range": "± 203.39950693723833"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 133797.41908482142,
            "unit": "ns",
            "range": "± 463.08381822282087"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 176750.66092354912,
            "unit": "ns",
            "range": "± 344.83074709035236"
          }
        ]
      }
    ],
    "Operations.SortedSetOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1747777313657,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 153365.65751139322,
            "unit": "ns",
            "range": "± 814.8880920649766"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 10839.900946983924,
            "unit": "ns",
            "range": "± 27.796782963410184"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11920.907707722981,
            "unit": "ns",
            "range": "± 46.540300106184986"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 13037.738521030971,
            "unit": "ns",
            "range": "± 49.75821034661955"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 15987.837692260742,
            "unit": "ns",
            "range": "± 31.458051211812517"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 13562.865132141113,
            "unit": "ns",
            "range": "± 79.29192549903858"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 13711.729861668178,
            "unit": "ns",
            "range": "± 42.15555217841353"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 15114.814849853516,
            "unit": "ns",
            "range": "± 25.900435771223307"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 16986.134039742607,
            "unit": "ns",
            "range": "± 77.2926537398904"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 13226.247237141926,
            "unit": "ns",
            "range": "± 45.55662202681105"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 85533.77497209821,
            "unit": "ns",
            "range": "± 188.42721392427683"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 12149.025769914899,
            "unit": "ns",
            "range": "± 53.116262730884095"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 83966.94157307943,
            "unit": "ns",
            "range": "± 347.6069956245306"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 86241.37558419364,
            "unit": "ns",
            "range": "± 437.23721521617745"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 17102.667657034737,
            "unit": "ns",
            "range": "± 73.06475237310487"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12282.802877298991,
            "unit": "ns",
            "range": "± 35.678012858159285"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 15948.114362276518,
            "unit": "ns",
            "range": "± 17.36652988389975"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 11204.050776672364,
            "unit": "ns",
            "range": "± 41.62652994868893"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 84240.59916178386,
            "unit": "ns",
            "range": "± 395.3173942507603"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 86658.3494181315,
            "unit": "ns",
            "range": "± 310.5966191410633"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 86701.65237630208,
            "unit": "ns",
            "range": "± 293.39978951074744"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 12046.93446467473,
            "unit": "ns",
            "range": "± 6.228286645630724"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 13809.28182067871,
            "unit": "ns",
            "range": "± 70.48892101647458"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 12094.021714346749,
            "unit": "ns",
            "range": "± 36.00044017672728"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 13202.6016834804,
            "unit": "ns",
            "range": "± 34.86515590000741"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 17211.95679982503,
            "unit": "ns",
            "range": "± 11.241710476588189"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 176638.0053222656,
            "unit": "ns",
            "range": "± 650.3552605174925"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 43295.10932413737,
            "unit": "ns",
            "range": "± 129.64571176232235"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 71788.04444231305,
            "unit": "ns",
            "range": "± 258.152622004532"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 123606.29727877103,
            "unit": "ns",
            "range": "± 321.3427932575923"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 188424.5204031808,
            "unit": "ns",
            "range": "± 866.1932444790458"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 109603.11695556641,
            "unit": "ns",
            "range": "± 472.7422604746414"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 136406.33314615887,
            "unit": "ns",
            "range": "± 679.5073217381224"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 139334.26424153647,
            "unit": "ns",
            "range": "± 658.3806081912782"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 229462.12777273994,
            "unit": "ns",
            "range": "± 1655.7387508030533"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 94193.6812866211,
            "unit": "ns",
            "range": "± 461.1170809308034"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 305836.62931315106,
            "unit": "ns",
            "range": "± 6443.664570541023"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 62587.0641031901,
            "unit": "ns",
            "range": "± 330.0482960910542"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 202625.19908854167,
            "unit": "ns",
            "range": "± 1017.7045363273745"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 200670.3528157552,
            "unit": "ns",
            "range": "± 910.2647540476518"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 18044.144453938803,
            "unit": "ns",
            "range": "± 152.63080792075695"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 81100.52787562778,
            "unit": "ns",
            "range": "± 347.36993562535736"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 141113.99605887276,
            "unit": "ns",
            "range": "± 827.0906490261235"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 62161.37168782552,
            "unit": "ns",
            "range": "± 513.5843634709288"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 235574.5013997396,
            "unit": "ns",
            "range": "± 1669.8660837366715"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 229358.6763044085,
            "unit": "ns",
            "range": "± 3712.4799077424245"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 229717.47612847222,
            "unit": "ns",
            "range": "± 4742.947285953021"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 63583.48360188802,
            "unit": "ns",
            "range": "± 252.30778403379443"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 13163.215022277833,
            "unit": "ns",
            "range": "± 30.17863757139855"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 63637.147733248195,
            "unit": "ns",
            "range": "± 144.97821138859254"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 141970.38258579798,
            "unit": "ns",
            "range": "± 448.9611265388637"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 256494.32114955358,
            "unit": "ns",
            "range": "± 1076.0041552580499"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 152043.60986328125,
            "unit": "ns",
            "range": "± 699.9822197759199"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 41141.35370107798,
            "unit": "ns",
            "range": "± 80.25746894419834"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 69972.91327776227,
            "unit": "ns",
            "range": "± 195.1587224539782"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 115719.06552559989,
            "unit": "ns",
            "range": "± 348.67989878613184"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 177439.79457310267,
            "unit": "ns",
            "range": "± 377.13819909133"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 102339.44388427734,
            "unit": "ns",
            "range": "± 445.1305159858437"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 138324.24669596355,
            "unit": "ns",
            "range": "± 911.8442982998233"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 134470.97464192708,
            "unit": "ns",
            "range": "± 352.3644143606302"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 185585.53614908856,
            "unit": "ns",
            "range": "± 973.5734580793062"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 94558.02224527995,
            "unit": "ns",
            "range": "± 316.9405023437804"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 276882.18118489586,
            "unit": "ns",
            "range": "± 4298.861562394985"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 63276.00508626302,
            "unit": "ns",
            "range": "± 389.6275406606272"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 191372.2729143415,
            "unit": "ns",
            "range": "± 1411.8191460103806"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 188025.5100285457,
            "unit": "ns",
            "range": "± 744.3491735939597"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 17319.01338704427,
            "unit": "ns",
            "range": "± 94.29801024530448"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 84737.29252115886,
            "unit": "ns",
            "range": "± 355.3265281558189"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 134988.3935546875,
            "unit": "ns",
            "range": "± 373.06673154515016"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 55745.98831380208,
            "unit": "ns",
            "range": "± 227.0423361871041"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 223109.40775553384,
            "unit": "ns",
            "range": "± 432.5662122499407"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 215990.37124235733,
            "unit": "ns",
            "range": "± 5313.568609259395"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 216122.0148111979,
            "unit": "ns",
            "range": "± 5444.124261890614"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 60842.65556640625,
            "unit": "ns",
            "range": "± 255.18457881353152"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 13802.56950785319,
            "unit": "ns",
            "range": "± 48.13374261620558"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 63852.29742838542,
            "unit": "ns",
            "range": "± 272.5065940406695"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 134137.71895926338,
            "unit": "ns",
            "range": "± 858.1339786770802"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 209862.56756998698,
            "unit": "ns",
            "range": "± 1078.0209423905178"
          }
        ]
      }
    ],
    "Operations.SortedSetOperations (windows-latest  net8.0 Release)": [
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
        "date": 1747777841741,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 127674.81282552083,
            "unit": "ns",
            "range": "± 158.6992953244642"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11792.486928304037,
            "unit": "ns",
            "range": "± 38.087708906519104"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 12063.004420353816,
            "unit": "ns",
            "range": "± 16.023756758195656"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 16236.554173060826,
            "unit": "ns",
            "range": "± 9.781321800751751"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 22927.974524864785,
            "unit": "ns",
            "range": "± 22.20125648292381"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 16973.668343680245,
            "unit": "ns",
            "range": "± 20.441004812477182"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 18712.543596540178,
            "unit": "ns",
            "range": "± 14.988562026755336"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 24235.03676194411,
            "unit": "ns",
            "range": "± 30.463108480025763"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 28330.537668863934,
            "unit": "ns",
            "range": "± 13.764672646292151"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 17408.565412248885,
            "unit": "ns",
            "range": "± 47.6627050304103"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 75727.00383112981,
            "unit": "ns",
            "range": "± 119.17798189823777"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15683.867797851562,
            "unit": "ns",
            "range": "± 10.056078711474871"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 74709.86676897321,
            "unit": "ns",
            "range": "± 98.87243099219639"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 68828.27962239583,
            "unit": "ns",
            "range": "± 128.4740804611904"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 12989.538465227399,
            "unit": "ns",
            "range": "± 30.495994022791546"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 13082.870156424386,
            "unit": "ns",
            "range": "± 11.459823866330895"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 25880.533708844865,
            "unit": "ns",
            "range": "± 17.316959713413382"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 12368.401336669922,
            "unit": "ns",
            "range": "± 6.148307349141471"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 82737.29060246394,
            "unit": "ns",
            "range": "± 103.1448414824617"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 75327.3555501302,
            "unit": "ns",
            "range": "± 201.05434576336884"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 75876.89534505208,
            "unit": "ns",
            "range": "± 129.15954775500478"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 14130.097045898438,
            "unit": "ns",
            "range": "± 13.938598710605513"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 9384.263719831195,
            "unit": "ns",
            "range": "± 9.573072007080604"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 14783.835728963217,
            "unit": "ns",
            "range": "± 6.026858845256573"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 15695.333426339286,
            "unit": "ns",
            "range": "± 17.498567506546497"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 29715.2836390904,
            "unit": "ns",
            "range": "± 31.575068957161342"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 134299.5166015625,
            "unit": "ns",
            "range": "± 438.7007656078123"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 37643.487548828125,
            "unit": "ns",
            "range": "± 79.71371651092035"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 62112.31776646205,
            "unit": "ns",
            "range": "± 91.07855528455768"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 108659.51450892857,
            "unit": "ns",
            "range": "± 280.88781804008175"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 161926.68294270834,
            "unit": "ns",
            "range": "± 861.7154778553984"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 94565.13631184895,
            "unit": "ns",
            "range": "± 257.7487937801148"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 124666.97102864583,
            "unit": "ns",
            "range": "± 661.1887848598237"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 126733.2292829241,
            "unit": "ns",
            "range": "± 874.7789056295305"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 208177.38606770834,
            "unit": "ns",
            "range": "± 750.6844254961331"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 91057.99473353794,
            "unit": "ns",
            "range": "± 237.46355089158993"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 270136.86930338544,
            "unit": "ns",
            "range": "± 2307.011119890659"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 59504.84183175223,
            "unit": "ns",
            "range": "± 90.79530185178061"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 162881.40024038462,
            "unit": "ns",
            "range": "± 394.23693161102943"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 172104.75698617788,
            "unit": "ns",
            "range": "± 616.1493949039756"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 13079.952675955636,
            "unit": "ns",
            "range": "± 24.705228983218376"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 74331.01545061384,
            "unit": "ns",
            "range": "± 278.00364988164336"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 123111.083984375,
            "unit": "ns",
            "range": "± 715.0677995383751"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 55650.87149483817,
            "unit": "ns",
            "range": "± 141.53656121126915"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 206530.40597098213,
            "unit": "ns",
            "range": "± 858.0589425064095"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 208517.10379464287,
            "unit": "ns",
            "range": "± 1022.7994898296203"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 219321.62039620537,
            "unit": "ns",
            "range": "± 1023.3494015972216"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 55072.22025553385,
            "unit": "ns",
            "range": "± 127.4680921902678"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 9557.992248535156,
            "unit": "ns",
            "range": "± 27.783924903180857"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 59384.27992600661,
            "unit": "ns",
            "range": "± 129.00926402836166"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 128434.2529296875,
            "unit": "ns",
            "range": "± 760.0351876417103"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 224841.88313802084,
            "unit": "ns",
            "range": "± 667.0372601952881"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 120046.36067708333,
            "unit": "ns",
            "range": "± 209.00358908129684"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 39055.18290201823,
            "unit": "ns",
            "range": "± 57.22601025589392"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 64804.31344168527,
            "unit": "ns",
            "range": "± 241.94931210595257"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 100630.35975864956,
            "unit": "ns",
            "range": "± 247.28396605149715"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 153932.06217447916,
            "unit": "ns",
            "range": "± 428.58050247740147"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 87300.3662109375,
            "unit": "ns",
            "range": "± 211.1475710731399"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 116376.26220703125,
            "unit": "ns",
            "range": "± 487.63580692105756"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 119602.44222005208,
            "unit": "ns",
            "range": "± 312.88545353085914"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 173542.61850210337,
            "unit": "ns",
            "range": "± 243.809596105236"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 94914.24909319196,
            "unit": "ns",
            "range": "± 320.5893450318332"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 237914.2333984375,
            "unit": "ns",
            "range": "± 2447.7504514192683"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 60183.5688664363,
            "unit": "ns",
            "range": "± 148.301977323784"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 159625.7831280048,
            "unit": "ns",
            "range": "± 396.38846798223335"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 156782.08182198662,
            "unit": "ns",
            "range": "± 399.688037561111"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 13074.23575265067,
            "unit": "ns",
            "range": "± 29.30234551008253"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 75800.73678152902,
            "unit": "ns",
            "range": "± 335.681011363846"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 115104.02018229167,
            "unit": "ns",
            "range": "± 228.54938043371405"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 53959.75443522135,
            "unit": "ns",
            "range": "± 94.06919729710138"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 202038.73116629463,
            "unit": "ns",
            "range": "± 761.9562738765259"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 195353.2435825893,
            "unit": "ns",
            "range": "± 723.9211424395787"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 199186.24674479166,
            "unit": "ns",
            "range": "± 857.5072790832833"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 56593.90493539663,
            "unit": "ns",
            "range": "± 105.94190019363386"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 9434.76791381836,
            "unit": "ns",
            "range": "± 26.12693402459902"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 60158.96993001302,
            "unit": "ns",
            "range": "± 118.86632411708837"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 119739.42626953125,
            "unit": "ns",
            "range": "± 329.4773221270474"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 186630.34842354912,
            "unit": "ns",
            "range": "± 366.5265582904607"
          }
        ]
      }
    ]
  }
}