window.BENCHMARK_DATA = {
  "lastUpdate": 1744675685501,
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
    ],
    "Lua.LuaRunnerOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675213478,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3941.9894736842107,
            "unit": "ns",
            "range": "± 1109.276450921441"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3875.8478260869565,
            "unit": "ns",
            "range": "± 670.819761495444"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 366977.92424242425,
            "unit": "ns",
            "range": "± 44291.74747823035"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 366618.1391752577,
            "unit": "ns",
            "range": "± 41721.867080102005"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 16655.967391304348,
            "unit": "ns",
            "range": "± 2397.8064355201172"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 138934.56043956045,
            "unit": "ns",
            "range": "± 14131.050937727165"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3374.5,
            "unit": "ns",
            "range": "± 62.996642157157254"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3951.3522727272725,
            "unit": "ns",
            "range": "± 667.4502442133964"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 336831.5081967213,
            "unit": "ns",
            "range": "± 14240.806222054227"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 380608.595959596,
            "unit": "ns",
            "range": "± 31605.133580660786"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 16641.86746987952,
            "unit": "ns",
            "range": "± 1828.2838265745972"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 149326.1326530612,
            "unit": "ns",
            "range": "± 20742.006089857998"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3388.0113636363635,
            "unit": "ns",
            "range": "± 375.3412392029352"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 4753.336734693878,
            "unit": "ns",
            "range": "± 1393.2163166674454"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 330004.2916666667,
            "unit": "ns",
            "range": "± 8337.6090331397"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 336046.3275862069,
            "unit": "ns",
            "range": "± 9794.446630138958"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 17165.64285714286,
            "unit": "ns",
            "range": "± 2538.6911348451067"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 141783.73958333334,
            "unit": "ns",
            "range": "± 14167.209355442696"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3780.6063829787236,
            "unit": "ns",
            "range": "± 484.3989784066437"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 4037.284090909091,
            "unit": "ns",
            "range": "± 769.0965209871688"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 443857.55555555556,
            "unit": "ns",
            "range": "± 9454.968436953708"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 438607.1666666667,
            "unit": "ns",
            "range": "± 20666.62789030688"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 19808.52688172043,
            "unit": "ns",
            "range": "± 3266.0618440835146"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 152114.74226804124,
            "unit": "ns",
            "range": "± 19156.945118045278"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3802.18085106383,
            "unit": "ns",
            "range": "± 749.6031381942485"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 4063.141304347826,
            "unit": "ns",
            "range": "± 447.92566056889643"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 429836.05,
            "unit": "ns",
            "range": "± 9857.585518803593"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 436512.8947368421,
            "unit": "ns",
            "range": "± 9678.665546762213"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 19337.73157894737,
            "unit": "ns",
            "range": "± 2940.7580201428877"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 145889.02197802198,
            "unit": "ns",
            "range": "± 15718.497318607373"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675225278,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 135645.45331682477,
            "unit": "ns",
            "range": "± 325.7987698543306"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 153424.1680063101,
            "unit": "ns",
            "range": "± 552.8521315165945"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 135790.18494591347,
            "unit": "ns",
            "range": "± 644.4900805192793"
          }
        ]
      }
    ],
    "Lua.LuaRunnerOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675221648,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3404.7065217391305,
            "unit": "ns",
            "range": "± 684.4310469134035"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3379.0652173913045,
            "unit": "ns",
            "range": "± 452.24971960938285"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 437126.3298969072,
            "unit": "ns",
            "range": "± 61652.2716719076"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 394639.72222222225,
            "unit": "ns",
            "range": "± 52889.49747515667"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 14481.5,
            "unit": "ns",
            "range": "± 165.64062962274136"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 162457.89583333334,
            "unit": "ns",
            "range": "± 23324.66041969954"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3467.3092783505153,
            "unit": "ns",
            "range": "± 457.262788248926"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3546.7582417582416,
            "unit": "ns",
            "range": "± 528.0265006114611"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 398042.0909090909,
            "unit": "ns",
            "range": "± 57254.60720630956"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 366910.1710526316,
            "unit": "ns",
            "range": "± 17187.12021671124"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 17114.978260869564,
            "unit": "ns",
            "range": "± 3627.559840323328"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 157231.15151515152,
            "unit": "ns",
            "range": "± 22609.15860991376"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3343.478723404255,
            "unit": "ns",
            "range": "± 511.6682335878092"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 3494.757894736842,
            "unit": "ns",
            "range": "± 573.3188507203173"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 347240.6037735849,
            "unit": "ns",
            "range": "± 13745.913937407031"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 410784.3775510204,
            "unit": "ns",
            "range": "± 64478.062549317634"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 16159.076086956522,
            "unit": "ns",
            "range": "± 1422.2974854102429"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 148631.1326530612,
            "unit": "ns",
            "range": "± 17568.38084685552"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3629.242105263158,
            "unit": "ns",
            "range": "± 498.3757173610038"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3490.3229166666665,
            "unit": "ns",
            "range": "± 550.2404927759777"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 466408.8181818182,
            "unit": "ns",
            "range": "± 11464.673436822657"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 546163.51,
            "unit": "ns",
            "range": "± 103697.34913938375"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 21836.625,
            "unit": "ns",
            "range": "± 2378.6019235324316"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 156645.52525252526,
            "unit": "ns",
            "range": "± 22975.687602396738"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3915.9555555555557,
            "unit": "ns",
            "range": "± 262.93374698936555"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3610.4242424242425,
            "unit": "ns",
            "range": "± 202.02361328899232"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 450953.7297297297,
            "unit": "ns",
            "range": "± 15307.822210027585"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 453264.17741935485,
            "unit": "ns",
            "range": "± 13799.113008661334"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 20340.977777777778,
            "unit": "ns",
            "range": "± 1973.8722109393511"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 159684.65979381444,
            "unit": "ns",
            "range": "± 17771.670223095825"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675227078,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 17686.026607806867,
            "unit": "ns",
            "range": "± 21.38504614236341"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 18405.799168178015,
            "unit": "ns",
            "range": "± 43.637446789668076"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 17909.192756144206,
            "unit": "ns",
            "range": "± 66.72214805129944"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675220938,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1248.5185185185185,
            "unit": "ns",
            "range": "± 44.04488127979875"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 1014.7216494845361,
            "unit": "ns",
            "range": "± 398.1061872129733"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1688.936170212766,
            "unit": "ns",
            "range": "± 502.5722824706468"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 466496.90476190473,
            "unit": "ns",
            "range": "± 10869.44204595968"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1743.2391304347825,
            "unit": "ns",
            "range": "± 355.2777081109287"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 22635.77,
            "unit": "ns",
            "range": "± 9888.00886274876"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 893.0947368421052,
            "unit": "ns",
            "range": "± 508.6647324892767"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 945.7315789473685,
            "unit": "ns",
            "range": "± 324.68070903881625"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1753.0274725274726,
            "unit": "ns",
            "range": "± 372.5500813659328"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 512093.69,
            "unit": "ns",
            "range": "± 65316.51984343996"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 1719.4684210526316,
            "unit": "ns",
            "range": "± 619.7740400293391"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 10825.924731182795,
            "unit": "ns",
            "range": "± 4425.563266131657"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 667.4578947368421,
            "unit": "ns",
            "range": "± 514.8167080516498"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 811.4432989690722,
            "unit": "ns",
            "range": "± 520.492854759477"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 2166.7903225806454,
            "unit": "ns",
            "range": "± 836.5801967929907"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 397219,
            "unit": "ns",
            "range": "± 6678.011123937169"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 1104.9166666666667,
            "unit": "ns",
            "range": "± 19.052360070144175"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 8935.744444444445,
            "unit": "ns",
            "range": "± 1216.0014281584522"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1015.5860215053764,
            "unit": "ns",
            "range": "± 467.50191015452543"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 992.1421052631579,
            "unit": "ns",
            "range": "± 336.26556487868373"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1810.2472527472528,
            "unit": "ns",
            "range": "± 415.30656931975744"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 487682.35,
            "unit": "ns",
            "range": "± 11046.731387371417"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1572.421875,
            "unit": "ns",
            "range": "± 868.9290696269536"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 11401.040816326531,
            "unit": "ns",
            "range": "± 3168.2105137035314"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1111.6195652173913,
            "unit": "ns",
            "range": "± 466.4595866595755"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 1518.1546391752577,
            "unit": "ns",
            "range": "± 660.777719500006"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 3637.547368421053,
            "unit": "ns",
            "range": "± 1449.3357742642488"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 614553.77,
            "unit": "ns",
            "range": "± 101072.96819623471"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 3114.705263157895,
            "unit": "ns",
            "range": "± 1698.3390986846355"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 20880.515151515152,
            "unit": "ns",
            "range": "± 7958.2766509037065"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675224010,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 18527.890154157365,
            "unit": "ns",
            "range": "± 55.017841105066005"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 18249.26365152995,
            "unit": "ns",
            "range": "± 86.37005440409645"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 18296.79940678523,
            "unit": "ns",
            "range": "± 73.46771770923436"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675245309,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 37351.03637084961,
            "unit": "ns",
            "range": "± 33.23892889018069"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 39312.802882267875,
            "unit": "ns",
            "range": "± 116.49211806080255"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 32304.814719063896,
            "unit": "ns",
            "range": "± 41.66317448305408"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 32835.82552664621,
            "unit": "ns",
            "range": "± 208.11570236030934"
          }
        ]
      }
    ],
    "Operations.BasicOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675250257,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1658.631365639823,
            "unit": "ns",
            "range": "± 10.481130427717908"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1622.38277053833,
            "unit": "ns",
            "range": "± 8.119733273474091"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1692.7033429463704,
            "unit": "ns",
            "range": "± 12.945403719863025"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675254550,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 100322.99509974888,
            "unit": "ns",
            "range": "± 1015.5783991414671"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 114525.05335286459,
            "unit": "ns",
            "range": "± 681.0953755196371"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 99117.43266194662,
            "unit": "ns",
            "range": "± 732.7528782887637"
          }
        ]
      }
    ],
    "Operations.BasicOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675257418,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1776.6838438851494,
            "unit": "ns",
            "range": "± 9.030413980114629"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1770.59941306481,
            "unit": "ns",
            "range": "± 0.9702677304713061"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1567.180599975586,
            "unit": "ns",
            "range": "± 11.808430219291132"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675250110,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 37189.891540527344,
            "unit": "ns",
            "range": "± 215.51690709629096"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 35969.94317392202,
            "unit": "ns",
            "range": "± 59.647070612068305"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 30372.27279428335,
            "unit": "ns",
            "range": "± 20.31573158170616"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30012.546914236886,
            "unit": "ns",
            "range": "± 79.59111153764388"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675247497,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1491.4239130434783,
            "unit": "ns",
            "range": "± 689.3896206733984"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 1291.157894736842,
            "unit": "ns",
            "range": "± 864.0803291532352"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 2086.306818181818,
            "unit": "ns",
            "range": "± 841.5864583330281"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 449584.09523809527,
            "unit": "ns",
            "range": "± 10690.013381211278"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 2104.771739130435,
            "unit": "ns",
            "range": "± 961.9896403826475"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 12597.36559139785,
            "unit": "ns",
            "range": "± 3818.714095497463"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1160.53125,
            "unit": "ns",
            "range": "± 705.4413467822822"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 876.4560439560439,
            "unit": "ns",
            "range": "± 392.3694379334045"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 2427.934065934066,
            "unit": "ns",
            "range": "± 797.1170526368105"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 433199.17741935485,
            "unit": "ns",
            "range": "± 31580.578000245554"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 2276.458762886598,
            "unit": "ns",
            "range": "± 1291.9671364299945"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 10630.21590909091,
            "unit": "ns",
            "range": "± 2685.53255468496"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 861.2916666666666,
            "unit": "ns",
            "range": "± 645.1554674595748"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 1003.0274725274726,
            "unit": "ns",
            "range": "± 447.8270584018726"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1954.287234042553,
            "unit": "ns",
            "range": "± 1020.635819354414"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 399232.7923076923,
            "unit": "ns",
            "range": "± 18514.4595930477"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 2638.963157894737,
            "unit": "ns",
            "range": "± 1344.981395193054"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 16364.19,
            "unit": "ns",
            "range": "± 9152.242222035435"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1051.9,
            "unit": "ns",
            "range": "± 484.60450224436175"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 855.2127659574468,
            "unit": "ns",
            "range": "± 396.4281280326776"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1860.0631578947368,
            "unit": "ns",
            "range": "± 819.3063206350082"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 465109.3076923077,
            "unit": "ns",
            "range": "± 12431.98151790528"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 3035.296703296703,
            "unit": "ns",
            "range": "± 1111.1151704931951"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 14475.214285714286,
            "unit": "ns",
            "range": "± 5762.354583893243"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1372.0773195876288,
            "unit": "ns",
            "range": "± 602.9453028511297"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 902.09375,
            "unit": "ns",
            "range": "± 617.5995394415552"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 2458.234693877551,
            "unit": "ns",
            "range": "± 1780.5268567617582"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 465540.78571428574,
            "unit": "ns",
            "range": "± 13096.97740887863"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 2526.7849462365593,
            "unit": "ns",
            "range": "± 1142.0889979927738"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 12097.81182795699,
            "unit": "ns",
            "range": "± 3834.5204875052764"
          }
        ]
      }
    ],
    "Network.BasicOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675273177,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 82.44408794811794,
            "unit": "ns",
            "range": "± 0.20372130410344744"
          }
        ]
      }
    ],
    "Network.BasicOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675326250,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 70.04866855485099,
            "unit": "ns",
            "range": "± 0.1196052590305903"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (windows-latest  net9.0 Release)": [
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
        "date": 1744675325658,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 34582.81930776743,
            "unit": "ns",
            "range": "± 35.14395203233234"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 35495.06131685697,
            "unit": "ns",
            "range": "± 61.70114721125198"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 30877.810872395832,
            "unit": "ns",
            "range": "± 28.092590447017802"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30921.279907226562,
            "unit": "ns",
            "range": "± 34.264941204198635"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675345576,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 16089.790461613582,
            "unit": "ns",
            "range": "± 8.72654167418916"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 16044.64345296224,
            "unit": "ns",
            "range": "± 44.712703852809504"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 16353.816441127232,
            "unit": "ns",
            "range": "± 53.5325245585258"
          }
        ]
      }
    ],
    "Operations.BasicOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675331955,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1736.018453325544,
            "unit": "ns",
            "range": "± 2.547471751737827"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1673.4702477088342,
            "unit": "ns",
            "range": "± 1.5881093472105998"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1675.1003632178674,
            "unit": "ns",
            "range": "± 3.370648076859694"
          }
        ]
      }
    ],
    "Cluster.ClusterMigrate (windows-latest  net8.0 Release)": [
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
        "date": 1744675342322,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 35168.565368652344,
            "unit": "ns",
            "range": "± 26.047947893728317"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 38862.15022160457,
            "unit": "ns",
            "range": "± 69.01915310725703"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 31055.320957728796,
            "unit": "ns",
            "range": "± 35.52503529021284"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30577.287074497766,
            "unit": "ns",
            "range": "± 29.34431748063067"
          }
        ]
      }
    ],
    "Operations.PubSubOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675346507,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15652.38015311105,
            "unit": "ns",
            "range": "± 11.336207650315487"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15663.721290001502,
            "unit": "ns",
            "range": "± 24.901248913571106"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 15794.99032156808,
            "unit": "ns",
            "range": "± 21.5880937062751"
          }
        ]
      }
    ],
    "Network.RawStringOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675374041,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 256.63909123494074,
            "unit": "ns",
            "range": "± 0.3710028623095106"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 277.46869703439563,
            "unit": "ns",
            "range": "± 0.5903533652188884"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 283.8263148625692,
            "unit": "ns",
            "range": "± 1.9020176905389632"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 315.76847964066724,
            "unit": "ns",
            "range": "± 0.7278750344577943"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 234.24495029449463,
            "unit": "ns",
            "range": "± 0.8064418273465442"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 184.73865827492304,
            "unit": "ns",
            "range": "± 0.9075775145695397"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 299.884359053203,
            "unit": "ns",
            "range": "± 1.672867800418839"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 299.68872626622516,
            "unit": "ns",
            "range": "± 1.6707464075740381"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 362.6212881895212,
            "unit": "ns",
            "range": "± 0.4510128496243437"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 362.4292257513319,
            "unit": "ns",
            "range": "± 3.080273786413722"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675370560,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 106196.23600886419,
            "unit": "ns",
            "range": "± 251.33891900601893"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 119200.79157902644,
            "unit": "ns",
            "range": "± 418.81682285769233"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 103708.0586751302,
            "unit": "ns",
            "range": "± 164.14934186839042"
          }
        ]
      }
    ],
    "Network.RawStringOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675379570,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 229.63281692908362,
            "unit": "ns",
            "range": "± 0.650147695001653"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 289.8080919470106,
            "unit": "ns",
            "range": "± 1.4185240706212157"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 325.6079711516698,
            "unit": "ns",
            "range": "± 0.39099216516915786"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 325.4405552148819,
            "unit": "ns",
            "range": "± 0.5768874569903011"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 240.77669488466702,
            "unit": "ns",
            "range": "± 0.4448814376942105"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 185.14373321716602,
            "unit": "ns",
            "range": "± 0.242285343789799"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 316.5446599080012,
            "unit": "ns",
            "range": "± 2.390520366733711"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 301.3224487622579,
            "unit": "ns",
            "range": "± 1.997010999898935"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 379.5326307296753,
            "unit": "ns",
            "range": "± 1.3072781819018944"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 372.76779486338296,
            "unit": "ns",
            "range": "± 3.275947281076841"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675389796,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 17188.09038035075,
            "unit": "ns",
            "range": "± 22.97078122332872"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 17404.807858980617,
            "unit": "ns",
            "range": "± 45.558955669150265"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 15548.211457472582,
            "unit": "ns",
            "range": "± 16.989271560531094"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 14287.380091349283,
            "unit": "ns",
            "range": "± 53.08370838989237"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 114461.95263671875,
            "unit": "ns",
            "range": "± 329.12170686489384"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 20724.025310809797,
            "unit": "ns",
            "range": "± 35.26707839246143"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 20646.188588460285,
            "unit": "ns",
            "range": "± 21.06292384798913"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 16408.890676879884,
            "unit": "ns",
            "range": "± 81.53798398837682"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 15060.606007167271,
            "unit": "ns",
            "range": "± 42.03296343173266"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 123865.20867919922,
            "unit": "ns",
            "range": "± 255.1185293702637"
          }
        ]
      }
    ],
    "Operations.ObjectOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675364510,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 96143.82760184152,
            "unit": "ns",
            "range": "± 150.57875253351585"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 109324.47591145833,
            "unit": "ns",
            "range": "± 1212.1080983855952"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 93924.93204752605,
            "unit": "ns",
            "range": "± 252.08202611024305"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675397214,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16256.251754760742,
            "unit": "ns",
            "range": "± 21.16056062650093"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 15218.833995056153,
            "unit": "ns",
            "range": "± 122.01575010071655"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14737.080422973633,
            "unit": "ns",
            "range": "± 95.22439030247658"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13262.344650268555,
            "unit": "ns",
            "range": "± 9.372592364674516"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 110612.89482770648,
            "unit": "ns",
            "range": "± 193.23254587455745"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19464.310836791992,
            "unit": "ns",
            "range": "± 17.526738281040828"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 18779.7978715163,
            "unit": "ns",
            "range": "± 9.760995952482906"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15791.994337815504,
            "unit": "ns",
            "range": "± 11.777805062514648"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 14563.2845186506,
            "unit": "ns",
            "range": "± 73.19646699717846"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 119272.1073843149,
            "unit": "ns",
            "range": "± 258.9419378025652"
          }
        ]
      }
    ],
    "Operations.BasicOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675398057,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1643.6489582061768,
            "unit": "ns",
            "range": "± 1.4124736878564907"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1711.815437903771,
            "unit": "ns",
            "range": "± 2.368097903059267"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1733.6057296166052,
            "unit": "ns",
            "range": "± 3.4631855851490534"
          }
        ]
      }
    ],
    "Operations.CustomOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675406889,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 34145.73891977163,
            "unit": "ns",
            "range": "± 30.52429193861668"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 164004.37191336494,
            "unit": "ns",
            "range": "± 617.0612313386466"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 110850.64244733538,
            "unit": "ns",
            "range": "± 384.96364098977784"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 78860.88981119792,
            "unit": "ns",
            "range": "± 118.64692397100188"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 32681.221108572823,
            "unit": "ns",
            "range": "± 203.46259947908527"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 177409.93368094307,
            "unit": "ns",
            "range": "± 1123.4011754763312"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 124970.12919921875,
            "unit": "ns",
            "range": "± 1289.6369847449544"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 109470.81331961496,
            "unit": "ns",
            "range": "± 216.2835862524427"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 34003.080627441406,
            "unit": "ns",
            "range": "± 49.63217353534372"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 154627.18600260417,
            "unit": "ns",
            "range": "± 962.8153984982296"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 110271.05120035807,
            "unit": "ns",
            "range": "± 431.5715625234093"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 77149.34142194476,
            "unit": "ns",
            "range": "± 261.63015475701604"
          }
        ]
      }
    ],
    "Operations.CustomOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675414622,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 31107.512135823566,
            "unit": "ns",
            "range": "± 61.89594794219244"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 137947.02627328725,
            "unit": "ns",
            "range": "± 184.39216446184827"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 111514.67842320034,
            "unit": "ns",
            "range": "± 314.2478497445513"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 72854.06331961496,
            "unit": "ns",
            "range": "± 393.6901979097211"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 31916.3511912028,
            "unit": "ns",
            "range": "± 62.969384828434876"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 146644.4548095703,
            "unit": "ns",
            "range": "± 835.8416947628791"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 120058.99516078403,
            "unit": "ns",
            "range": "± 471.923105068971"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 98960.15941975912,
            "unit": "ns",
            "range": "± 367.24010800557113"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 32738.16703913762,
            "unit": "ns",
            "range": "± 14.651299811801474"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 137866.75355747767,
            "unit": "ns",
            "range": "± 674.8867305560542"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 111954.55155726841,
            "unit": "ns",
            "range": "± 473.20231906023366"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 73997.38548990885,
            "unit": "ns",
            "range": "± 379.5015148932234"
          }
        ]
      }
    ],
    "Network.RawStringOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675483323,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 205.25014082590738,
            "unit": "ns",
            "range": "± 0.37150365299749416"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 276.7701307932536,
            "unit": "ns",
            "range": "± 0.6654183765808166"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 283.0822211045485,
            "unit": "ns",
            "range": "± 0.36034940478707167"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 295.81337656293596,
            "unit": "ns",
            "range": "± 0.3580924400267114"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 217.46862668257492,
            "unit": "ns",
            "range": "± 0.25123903666905173"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 153.14257144927979,
            "unit": "ns",
            "range": "± 0.24478038370635505"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 298.72360547383624,
            "unit": "ns",
            "range": "± 1.3962770423828255"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 305.27712277003695,
            "unit": "ns",
            "range": "± 0.9161348528901287"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 344.0848223368327,
            "unit": "ns",
            "range": "± 1.0629180067632202"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 341.928277696882,
            "unit": "ns",
            "range": "± 0.589993591583005"
          }
        ]
      }
    ]
  }
}