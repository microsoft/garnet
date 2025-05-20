window.BENCHMARK_DATA = {
  "lastUpdate": 1747775859481,
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
    ]
  }
}