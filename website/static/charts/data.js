window.BENCHMARK_DATA = {
  "lastUpdate": 1747776148199,
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
    ]
  }
}