window.BENCHMARK_DATA = {
  "lastUpdate": 1744676105430,
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
    ],
    "Cluster.ClusterOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675500599,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 15907.432861328125,
            "unit": "ns",
            "range": "± 34.53859242916826"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 14643.540802001953,
            "unit": "ns",
            "range": "± 22.766783595946222"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14494.695383707682,
            "unit": "ns",
            "range": "± 62.6406134159496"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13066.67960030692,
            "unit": "ns",
            "range": "± 13.154624738288696"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 104181.5691266741,
            "unit": "ns",
            "range": "± 223.77805635611807"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19525.819396972656,
            "unit": "ns",
            "range": "± 16.61839934606306"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19122.3148890904,
            "unit": "ns",
            "range": "± 22.705760906329083"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15787.911987304688,
            "unit": "ns",
            "range": "± 13.621485410044045"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 13859.34342604417,
            "unit": "ns",
            "range": "± 18.64868807070618"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 111207.97206333706,
            "unit": "ns",
            "range": "± 102.82200263713558"
          }
        ]
      }
    ],
    "Cluster.ClusterOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675506289,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 15986.115926106771,
            "unit": "ns",
            "range": "± 20.786041179256028"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 15024.965108235678,
            "unit": "ns",
            "range": "± 17.396286976721125"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14246.361424372746,
            "unit": "ns",
            "range": "± 12.701522495238626"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13485.509926932198,
            "unit": "ns",
            "range": "± 14.092009843224462"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 106128.72924804688,
            "unit": "ns",
            "range": "± 126.29709972579816"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 21096.590482271633,
            "unit": "ns",
            "range": "± 57.40359168468745"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19338.336617606026,
            "unit": "ns",
            "range": "± 27.17153100178531"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15535.829060872396,
            "unit": "ns",
            "range": "± 41.40017561406933"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 14490.035705566406,
            "unit": "ns",
            "range": "± 24.289835002276952"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 118725.48170823317,
            "unit": "ns",
            "range": "± 99.02526929207974"
          }
        ]
      }
    ],
    "Operations.CustomOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675520440,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 29002.289632161457,
            "unit": "ns",
            "range": "± 51.09810812902097"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 135061.00830078125,
            "unit": "ns",
            "range": "± 225.45131628411667"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 105373.27619280134,
            "unit": "ns",
            "range": "± 144.87022773801377"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 79642.06298828125,
            "unit": "ns",
            "range": "± 64.44071286070017"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 31102.490234375,
            "unit": "ns",
            "range": "± 43.31243693007561"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 148746.826171875,
            "unit": "ns",
            "range": "± 336.6951082594528"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 112258.31909179688,
            "unit": "ns",
            "range": "± 188.11570280989994"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 102441.89976283482,
            "unit": "ns",
            "range": "± 187.00661211700694"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 30837.46602376302,
            "unit": "ns",
            "range": "± 28.324695348235597"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 137397.15482271634,
            "unit": "ns",
            "range": "± 192.64520520548226"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 100221.73649714544,
            "unit": "ns",
            "range": "± 94.31988489986155"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 79510.28401692708,
            "unit": "ns",
            "range": "± 218.3826967272121"
          }
        ]
      }
    ],
    "Network.RawStringOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675526963,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 218.3893118585859,
            "unit": "ns",
            "range": "± 0.35844745263881506"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 275.0294061807486,
            "unit": "ns",
            "range": "± 0.3734588711249447"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 284.9798234303792,
            "unit": "ns",
            "range": "± 0.3934676311645666"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 320.444233076913,
            "unit": "ns",
            "range": "± 0.6834692992912681"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 220.39328893025717,
            "unit": "ns",
            "range": "± 0.4675975241931881"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 179.03625454221452,
            "unit": "ns",
            "range": "± 0.5239501810518866"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 289.66710408528644,
            "unit": "ns",
            "range": "± 0.918725151188151"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 297.3480002085368,
            "unit": "ns",
            "range": "± 0.43873565334050074"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 362.5612672170003,
            "unit": "ns",
            "range": "± 2.0661986397733916"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 353.25603118309607,
            "unit": "ns",
            "range": "± 1.7540125616125684"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675519519,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1868.131868131868,
            "unit": "ns",
            "range": "± 1805.4164875531333"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 1084.090909090909,
            "unit": "ns",
            "range": "± 579.7118514581215"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 3946.875,
            "unit": "ns",
            "range": "± 2266.6902894795867"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 477558,
            "unit": "ns",
            "range": "± 110235.39659576802"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 5810.227272727273,
            "unit": "ns",
            "range": "± 2348.8338888496496"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 14737.113402061856,
            "unit": "ns",
            "range": "± 2940.5682411323187"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 2305.1546391752577,
            "unit": "ns",
            "range": "± 2058.9424436477807"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 1948.9795918367347,
            "unit": "ns",
            "range": "± 1482.1442323041338"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 4534.693877551021,
            "unit": "ns",
            "range": "± 2666.2109506109987"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 483901.03092783503,
            "unit": "ns",
            "range": "± 93914.27772935336"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 5420.833333333333,
            "unit": "ns",
            "range": "± 2780.000631074016"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 14563.265306122448,
            "unit": "ns",
            "range": "± 2863.2181359155074"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1424.0506329113923,
            "unit": "ns",
            "range": "± 828.5320955164477"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 1984.5360824742268,
            "unit": "ns",
            "range": "± 1479.3255928368146"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 4080.9278350515465,
            "unit": "ns",
            "range": "± 2569.7503455565443"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 495723,
            "unit": "ns",
            "range": "± 94036.97086730831"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 5487.628865979382,
            "unit": "ns",
            "range": "± 3171.167560088567"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 14431.632653061224,
            "unit": "ns",
            "range": "± 3081.5913996483546"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 2313.40206185567,
            "unit": "ns",
            "range": "± 2053.585729176904"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 1406.3218390804598,
            "unit": "ns",
            "range": "± 836.3404710925306"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 4787.755102040816,
            "unit": "ns",
            "range": "± 2825.4099348999657"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 556734,
            "unit": "ns",
            "range": "± 127018.97513538851"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 5725.510204081633,
            "unit": "ns",
            "range": "± 3322.4730332383187"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 14587.5,
            "unit": "ns",
            "range": "± 3526.3295365010913"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 2476.0416666666665,
            "unit": "ns",
            "range": "± 1971.4138550084551"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 2193.684210526316,
            "unit": "ns",
            "range": "± 1477.6210413259955"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 3977.5510204081634,
            "unit": "ns",
            "range": "± 2396.8420207906506"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 488910.67415730335,
            "unit": "ns",
            "range": "± 77917.72411562424"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 5103.061224489796,
            "unit": "ns",
            "range": "± 3367.222358054786"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 14771.875,
            "unit": "ns",
            "range": "± 3182.3759123284635"
          }
        ]
      }
    ],
    "Lua.LuaRunnerOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675548241,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3213.0434782608695,
            "unit": "ns",
            "range": "± 983.1791214738312"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3455.5555555555557,
            "unit": "ns",
            "range": "± 1124.7083128588197"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 325472.44897959183,
            "unit": "ns",
            "range": "± 72337.39750970276"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 319626.2626262626,
            "unit": "ns",
            "range": "± 60531.94566585141"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 19318.367346938776,
            "unit": "ns",
            "range": "± 5795.400717112879"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 131371.13402061857,
            "unit": "ns",
            "range": "± 22526.396362392312"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3958.5858585858587,
            "unit": "ns",
            "range": "± 1480.0723990158701"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3266.304347826087,
            "unit": "ns",
            "range": "± 761.3286786296479"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 354717.7083333333,
            "unit": "ns",
            "range": "± 81905.88693551265"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 339011.2244897959,
            "unit": "ns",
            "range": "± 79006.77105610615"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 23537.755102040817,
            "unit": "ns",
            "range": "± 5732.535383125702"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 130923.71134020618,
            "unit": "ns",
            "range": "± 21052.324560860434"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3590.217391304348,
            "unit": "ns",
            "range": "± 809.5663990183224"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 3783.14606741573,
            "unit": "ns",
            "range": "± 711.949301542823"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 314962.6506024096,
            "unit": "ns",
            "range": "± 35770.93352949508"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 326362.35294117645,
            "unit": "ns",
            "range": "± 35624.670345912"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 20292.708333333332,
            "unit": "ns",
            "range": "± 3449.980771621982"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 127689.36170212766,
            "unit": "ns",
            "range": "± 20511.693324273718"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3880.21978021978,
            "unit": "ns",
            "range": "± 1051.0544737146154"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3425.8426966292136,
            "unit": "ns",
            "range": "± 662.7299069152671"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 437836.08247422683,
            "unit": "ns",
            "range": "± 89216.36876339713"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 472305,
            "unit": "ns",
            "range": "± 107418.39032869595"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 26776.530612244896,
            "unit": "ns",
            "range": "± 5680.474806283927"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 134608.51063829788,
            "unit": "ns",
            "range": "± 22749.116298916008"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3646.6666666666665,
            "unit": "ns",
            "range": "± 847.8922975912723"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3460.2272727272725,
            "unit": "ns",
            "range": "± 720.4787317650743"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 450961,
            "unit": "ns",
            "range": "± 103811.59789530146"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 421242.7083333333,
            "unit": "ns",
            "range": "± 87154.17403392159"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 26119.791666666668,
            "unit": "ns",
            "range": "± 4306.3874172813685"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 130817.8947368421,
            "unit": "ns",
            "range": "± 21714.947195213932"
          }
        ]
      }
    ],
    "Lua.LuaScripts (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675567435,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 270.0856906890869,
            "unit": "ns",
            "range": "± 1.6723262653226563"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 400.8867568333944,
            "unit": "ns",
            "range": "± 2.393526199270497"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 645.6914359410604,
            "unit": "ns",
            "range": "± 3.2724853520683124"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 868.8153061499962,
            "unit": "ns",
            "range": "± 2.705898142659278"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 282.91600946279675,
            "unit": "ns",
            "range": "± 0.23384889715944407"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 382.77840866361345,
            "unit": "ns",
            "range": "± 0.8075364782098682"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 647.861462465922,
            "unit": "ns",
            "range": "± 4.110606235536367"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 877.8575322287423,
            "unit": "ns",
            "range": "± 2.9969454706017142"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 282.47180436207697,
            "unit": "ns",
            "range": "± 0.23077732638517906"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 354.61314423878986,
            "unit": "ns",
            "range": "± 1.944954851283175"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 629.8469599315098,
            "unit": "ns",
            "range": "± 1.4840222292510152"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 864.6254903793335,
            "unit": "ns",
            "range": "± 1.8460230244604638"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 273.40209167344227,
            "unit": "ns",
            "range": "± 1.3488281962911262"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 358.1044381005423,
            "unit": "ns",
            "range": "± 1.246340991961222"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 633.9221641676767,
            "unit": "ns",
            "range": "± 1.4252496603984581"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 882.8726273854573,
            "unit": "ns",
            "range": "± 2.5439246175487034"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 273.17617495854694,
            "unit": "ns",
            "range": "± 0.2931303804736991"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 351.79449672698973,
            "unit": "ns",
            "range": "± 1.1391427791829607"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 616.8603451068585,
            "unit": "ns",
            "range": "± 1.4589162261462687"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 846.0477641650608,
            "unit": "ns",
            "range": "± 2.725893664782152"
          }
        ]
      }
    ],
    "Lua.LuaRunnerOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675557930,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 5845.698924731183,
            "unit": "ns",
            "range": "± 1754.4300436134845"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 7217.368421052632,
            "unit": "ns",
            "range": "± 2331.716658513188"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 335116,
            "unit": "ns",
            "range": "± 68109.49105460392"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 351962,
            "unit": "ns",
            "range": "± 70580.85810836485"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 28769.473684210527,
            "unit": "ns",
            "range": "± 6819.435205979705"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 134995.9595959596,
            "unit": "ns",
            "range": "± 29858.93336802073"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 6726.8421052631575,
            "unit": "ns",
            "range": "± 2878.6984585450364"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 7465.151515151515,
            "unit": "ns",
            "range": "± 2874.3185043700364"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 333446,
            "unit": "ns",
            "range": "± 64114.7636128475"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 340080,
            "unit": "ns",
            "range": "± 64322.55365803591"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 28584.375,
            "unit": "ns",
            "range": "± 7698.7740394511075"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 136329,
            "unit": "ns",
            "range": "± 29946.13445316221"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 8335.353535353535,
            "unit": "ns",
            "range": "± 3421.2813309001563"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 8720.430107526881,
            "unit": "ns",
            "range": "± 2073.9355121721846"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 376068,
            "unit": "ns",
            "range": "± 81575.76373091967"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 350362,
            "unit": "ns",
            "range": "± 71618.30684083138"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 26147.872340425532,
            "unit": "ns",
            "range": "± 6927.131925603163"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 143105.10204081633,
            "unit": "ns",
            "range": "± 28870.672782197107"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 7492.783505154639,
            "unit": "ns",
            "range": "± 2313.6032243505124"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 7252.127659574468,
            "unit": "ns",
            "range": "± 2098.577537870004"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 460007.14285714284,
            "unit": "ns",
            "range": "± 93647.80505571168"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 483208,
            "unit": "ns",
            "range": "± 111275.24675961919"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 37359.183673469386,
            "unit": "ns",
            "range": "± 8595.801381406895"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 153220.4081632653,
            "unit": "ns",
            "range": "± 35298.06793341082"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 6148.958333333333,
            "unit": "ns",
            "range": "± 2133.4427226614707"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 6811.578947368421,
            "unit": "ns",
            "range": "± 2441.218005984628"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 458206,
            "unit": "ns",
            "range": "± 102527.2221254808"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 410928.9156626506,
            "unit": "ns",
            "range": "± 58029.337681354795"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 32686.73469387755,
            "unit": "ns",
            "range": "± 11124.892101921818"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 148648.96907216494,
            "unit": "ns",
            "range": "± 27731.99676834415"
          }
        ]
      }
    ],
    "Lua.LuaScriptCacheOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675574189,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1688.2978723404256,
            "unit": "ns",
            "range": "± 1474.7958767436057"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 1589.3617021276596,
            "unit": "ns",
            "range": "± 1227.4169122050437"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 4295.833333333333,
            "unit": "ns",
            "range": "± 2242.3632583629565"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 425577.3195876289,
            "unit": "ns",
            "range": "± 70344.97806221765"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 4572.340425531915,
            "unit": "ns",
            "range": "± 2231.514469839014"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 16260,
            "unit": "ns",
            "range": "± 4079.6432885366494"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1348.3870967741937,
            "unit": "ns",
            "range": "± 1163.0977101301328"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 1201.0752688172042,
            "unit": "ns",
            "range": "± 876.1941694247312"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 3109.6774193548385,
            "unit": "ns",
            "range": "± 1637.6722953584372"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 437235.71428571426,
            "unit": "ns",
            "range": "± 86281.44190087951"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 4713.684210526316,
            "unit": "ns",
            "range": "± 1991.984497841702"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 14375.78947368421,
            "unit": "ns",
            "range": "± 4483.366796008062"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1450.5494505494505,
            "unit": "ns",
            "range": "± 1471.0822642133337"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 1409.7826086956522,
            "unit": "ns",
            "range": "± 1289.8854010438629"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 3520,
            "unit": "ns",
            "range": "± 2027.3974512985644"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 426695.9595959596,
            "unit": "ns",
            "range": "± 84942.74412465682"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 3869.4736842105262,
            "unit": "ns",
            "range": "± 2147.1442530601075"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 11273.737373737375,
            "unit": "ns",
            "range": "± 4938.408110415494"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1409.7826086956522,
            "unit": "ns",
            "range": "± 940.7805717008739"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 1145.360824742268,
            "unit": "ns",
            "range": "± 1049.7043848306266"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 2637.5,
            "unit": "ns",
            "range": "± 2190.9983304901275"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 483561,
            "unit": "ns",
            "range": "± 93581.9544624395"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 2353.191489361702,
            "unit": "ns",
            "range": "± 1968.171583549661"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 12116.326530612245,
            "unit": "ns",
            "range": "± 4870.0789357804215"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1581.25,
            "unit": "ns",
            "range": "± 1569.7678339906124"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 1075.8241758241759,
            "unit": "ns",
            "range": "± 1041.7230021396347"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 3215.625,
            "unit": "ns",
            "range": "± 2371.645137429868"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 429995.45454545453,
            "unit": "ns",
            "range": "± 49617.231223374125"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 3826.315789473684,
            "unit": "ns",
            "range": "± 2236.458566683658"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 15371.42857142857,
            "unit": "ns",
            "range": "± 5272.101245840387"
          }
        ]
      }
    ],
    "Operations.CustomOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675571117,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 32654.910714285714,
            "unit": "ns",
            "range": "± 52.69768329419451"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 155282.61893136162,
            "unit": "ns",
            "range": "± 482.26048440545304"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 104784.5670259916,
            "unit": "ns",
            "range": "± 102.1516894416536"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 82082.18383789062,
            "unit": "ns",
            "range": "± 54.39092064715091"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 31758.641764322918,
            "unit": "ns",
            "range": "± 460.90406128489275"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 165537.744140625,
            "unit": "ns",
            "range": "± 983.5276266067234"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 121037.18098958333,
            "unit": "ns",
            "range": "± 294.9164732333391"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 111374.09138997395,
            "unit": "ns",
            "range": "± 194.74823846337483"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 30943.993123372395,
            "unit": "ns",
            "range": "± 45.866142571844954"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 152268.39680989584,
            "unit": "ns",
            "range": "± 362.85399723195815"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 107754.67610677083,
            "unit": "ns",
            "range": "± 179.44452043113836"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 88769.46847098214,
            "unit": "ns",
            "range": "± 153.2693403898067"
          }
        ]
      }
    ],
    "Lua.LuaScripts (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675595580,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 295.5238862991333,
            "unit": "ns",
            "range": "± 0.2959590334354507"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 369.2168191160475,
            "unit": "ns",
            "range": "± 1.3527468249430799"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 628.4198057174683,
            "unit": "ns",
            "range": "± 2.8100361124125697"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 858.3138947120079,
            "unit": "ns",
            "range": "± 2.8671137062573653"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 277.54747148922513,
            "unit": "ns",
            "range": "± 0.3662134468734289"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 366.2994969074543,
            "unit": "ns",
            "range": "± 0.4386394374386133"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 634.5575093541827,
            "unit": "ns",
            "range": "± 1.9567309912331996"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 878.864245223999,
            "unit": "ns",
            "range": "± 2.175998043379922"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 295.0522890488307,
            "unit": "ns",
            "range": "± 0.27911836650639604"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 352.26463726588656,
            "unit": "ns",
            "range": "± 0.8953652290539459"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 648.9743309702192,
            "unit": "ns",
            "range": "± 1.6287824744233799"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 854.6397653579712,
            "unit": "ns",
            "range": "± 2.152376308872404"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 288.5525100390116,
            "unit": "ns",
            "range": "± 1.5723746112007175"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 370.73931779180253,
            "unit": "ns",
            "range": "± 1.0729991163926167"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 636.6408294677734,
            "unit": "ns",
            "range": "± 3.177943064060602"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 858.6588872273763,
            "unit": "ns",
            "range": "± 1.8866314576249497"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 281.2047005380903,
            "unit": "ns",
            "range": "± 0.35445344897141645"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 342.9397009100233,
            "unit": "ns",
            "range": "± 1.1281024901425114"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 648.3301640919277,
            "unit": "ns",
            "range": "± 2.966073868641964"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 856.235083770752,
            "unit": "ns",
            "range": "± 2.380589688533562"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675637950,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 32447.767295328777,
            "unit": "ns",
            "range": "± 310.99943633489795"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 39668.53151593889,
            "unit": "ns",
            "range": "± 224.2639225315259"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 75961.15163748605,
            "unit": "ns",
            "range": "± 625.4015530708411"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 55081.11747233073,
            "unit": "ns",
            "range": "± 386.550309955747"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 16352.028720092774,
            "unit": "ns",
            "range": "± 122.06250084876604"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 29264.815905253094,
            "unit": "ns",
            "range": "± 146.77061827646241"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 168124.7873860677,
            "unit": "ns",
            "range": "± 1017.6818473504574"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 314010.4943522135,
            "unit": "ns",
            "range": "± 2190.152878044585"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 30870.084720865885,
            "unit": "ns",
            "range": "± 246.2818923721865"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 46803.299552408855,
            "unit": "ns",
            "range": "± 302.1651132901992"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 84596.9379313151,
            "unit": "ns",
            "range": "± 351.11741295811964"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 57557.540239606584,
            "unit": "ns",
            "range": "± 93.67688697544085"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15673.639721210186,
            "unit": "ns",
            "range": "± 103.74086261315"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 37763.363653564455,
            "unit": "ns",
            "range": "± 304.0590921020844"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 160090.8396809896,
            "unit": "ns",
            "range": "± 403.2186791163286"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 315094.19013671874,
            "unit": "ns",
            "range": "± 1760.42540186985"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 30306.239412943523,
            "unit": "ns",
            "range": "± 56.68178303498429"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 40762.94316319057,
            "unit": "ns",
            "range": "± 217.72599304940167"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 73425.81735839843,
            "unit": "ns",
            "range": "± 257.75163887509694"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 54374.89499773298,
            "unit": "ns",
            "range": "± 157.9833210670524"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 16083.825197347005,
            "unit": "ns",
            "range": "± 144.2585317005423"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 28477.75156656901,
            "unit": "ns",
            "range": "± 160.08183617938076"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 166413.86432756696,
            "unit": "ns",
            "range": "± 695.8465770810036"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 306398.13406808034,
            "unit": "ns",
            "range": "± 2874.1003296830922"
          }
        ]
      }
    ],
    "Lua.LuaScripts (windows-latest  net9.0 Release)": [
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
        "date": 1744675697175,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 155.5826203028361,
            "unit": "ns",
            "range": "± 0.42612120156351857"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 194.83906428019205,
            "unit": "ns",
            "range": "± 0.4850005583090614"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 314.2885514668056,
            "unit": "ns",
            "range": "± 0.48992859941088907"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 358.4105219159807,
            "unit": "ns",
            "range": "± 0.5763592865919122"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 169.84699567159018,
            "unit": "ns",
            "range": "± 0.5255015234754575"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 189.79356459208898,
            "unit": "ns",
            "range": "± 0.22017010187216748"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 332.7111831078163,
            "unit": "ns",
            "range": "± 0.43209857563719123"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 355.424325806754,
            "unit": "ns",
            "range": "± 0.6167924830760354"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 161.81004842122397,
            "unit": "ns",
            "range": "± 0.18886728057814473"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 196.1786937713623,
            "unit": "ns",
            "range": "± 0.37734232574593807"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 313.3258138384138,
            "unit": "ns",
            "range": "± 0.341957658535557"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 353.97688792302057,
            "unit": "ns",
            "range": "± 0.4219693930680801"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 172.51503467559814,
            "unit": "ns",
            "range": "± 0.2573741917462195"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 190.03887517111642,
            "unit": "ns",
            "range": "± 0.23991361294632949"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 313.8935463769095,
            "unit": "ns",
            "range": "± 0.872492072273125"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 372.33298008258527,
            "unit": "ns",
            "range": "± 0.36026482971908147"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 158.1446679433187,
            "unit": "ns",
            "range": "± 0.41391643530840383"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 190.77305964061193,
            "unit": "ns",
            "range": "± 0.3325329617000068"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 312.10565933814416,
            "unit": "ns",
            "range": "± 0.3906253383132263"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 359.5704827989851,
            "unit": "ns",
            "range": "± 0.5492495611503322"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675694273,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 29506.131914578953,
            "unit": "ns",
            "range": "± 101.85656861301963"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 38001.45856221517,
            "unit": "ns",
            "range": "± 38.04025993671881"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 55084.662272135414,
            "unit": "ns",
            "range": "± 304.93039492692685"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 56525.132944742836,
            "unit": "ns",
            "range": "± 312.1943968275581"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 15117.262507120768,
            "unit": "ns",
            "range": "± 80.93484834119013"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 28639.80424194336,
            "unit": "ns",
            "range": "± 116.41310353506397"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 143455.08522135418,
            "unit": "ns",
            "range": "± 1148.2442222395484"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 237023.01386369977,
            "unit": "ns",
            "range": "± 856.1534595910753"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 29155.633524576824,
            "unit": "ns",
            "range": "± 196.6915860951087"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 46087.88458251953,
            "unit": "ns",
            "range": "± 162.01404120144653"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 61907.309870793266,
            "unit": "ns",
            "range": "± 295.6410439583019"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 59421.29596761068,
            "unit": "ns",
            "range": "± 215.9842050720797"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15855.781452433268,
            "unit": "ns",
            "range": "± 106.77182808955897"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 34076.453669956754,
            "unit": "ns",
            "range": "± 227.88347111281396"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 141480.66018880208,
            "unit": "ns",
            "range": "± 584.4605855018219"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 253746.5471516927,
            "unit": "ns",
            "range": "± 2284.4425476656556"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 28664.884326171876,
            "unit": "ns",
            "range": "± 126.41807780906137"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 38579.178388323104,
            "unit": "ns",
            "range": "± 185.37477498018592"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 53149.55984966572,
            "unit": "ns",
            "range": "± 138.17133226490688"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 59395.589463297525,
            "unit": "ns",
            "range": "± 215.45303232750175"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 15063.832121785481,
            "unit": "ns",
            "range": "± 68.27099325867017"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 28016.403190612793,
            "unit": "ns",
            "range": "± 27.732300874656794"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 147822.34205729168,
            "unit": "ns",
            "range": "± 1116.7535736937393"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 235973.57099609374,
            "unit": "ns",
            "range": "± 1490.4093647685493"
          }
        ]
      }
    ],
    "Lua.LuaScripts (windows-latest  net8.0 Release)": [
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
        "date": 1744675692201,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 162.69478247715875,
            "unit": "ns",
            "range": "± 0.19654664169702138"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 196.3518738746643,
            "unit": "ns",
            "range": "± 0.3191571864563119"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 334.70769950321744,
            "unit": "ns",
            "range": "± 0.7859022305009817"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 398.99138382502963,
            "unit": "ns",
            "range": "± 0.6880764013816769"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 157.73483789884128,
            "unit": "ns",
            "range": "± 0.5093319898201892"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 196.54712310204138,
            "unit": "ns",
            "range": "± 0.23771401247188567"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 329.17507716587613,
            "unit": "ns",
            "range": "± 0.4524424868722508"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 350.705607732137,
            "unit": "ns",
            "range": "± 0.6630394021528639"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 160.6059500149318,
            "unit": "ns",
            "range": "± 0.2411646551713676"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 188.8163106782096,
            "unit": "ns",
            "range": "± 0.36039958461377397"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 321.42660935719806,
            "unit": "ns",
            "range": "± 0.8223815086874633"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 355.1013708114624,
            "unit": "ns",
            "range": "± 0.5913068536441802"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 160.761127105126,
            "unit": "ns",
            "range": "± 0.3134758063774719"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 192.514599164327,
            "unit": "ns",
            "range": "± 0.3987410514441145"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 319.56844329833984,
            "unit": "ns",
            "range": "± 0.9273624965113915"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 371.78241184779574,
            "unit": "ns",
            "range": "± 1.2352238551885817"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 157.91101296742758,
            "unit": "ns",
            "range": "± 0.39321424550642975"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 190.51968867962177,
            "unit": "ns",
            "range": "± 0.22947306327828165"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 312.6220498766218,
            "unit": "ns",
            "range": "± 0.4962630552931705"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 359.824868610927,
            "unit": "ns",
            "range": "± 0.31576894507872894"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675752992,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 15048.166157313755,
            "unit": "ns",
            "range": "± 133.0534177578531"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 19769.12820180257,
            "unit": "ns",
            "range": "± 21.368389170652563"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 21650.42451985677,
            "unit": "ns",
            "range": "± 98.51003104473939"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 22835.549146379744,
            "unit": "ns",
            "range": "± 135.34615204649756"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 16489.283451960637,
            "unit": "ns",
            "range": "± 14.398215821492947"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 10560.4354494535,
            "unit": "ns",
            "range": "± 13.867701614587553"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 21641.879309517997,
            "unit": "ns",
            "range": "± 72.79844219283395"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 22711.566068522134,
            "unit": "ns",
            "range": "± 89.83598834598465"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 27676.055728149415,
            "unit": "ns",
            "range": "± 116.12387450723652"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 27033.04463849749,
            "unit": "ns",
            "range": "± 100.24439177715563"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 21512.581340244837,
            "unit": "ns",
            "range": "± 83.06623262465617"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26457.647989908855,
            "unit": "ns",
            "range": "± 141.68379371002055"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 29155.31226675851,
            "unit": "ns",
            "range": "± 94.87192314007159"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 30880.145256629356,
            "unit": "ns",
            "range": "± 97.81812524891774"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 16462.788271222795,
            "unit": "ns",
            "range": "± 15.90236476807508"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10402.20359395345,
            "unit": "ns",
            "range": "± 46.80512375444942"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 28498.60180882045,
            "unit": "ns",
            "range": "± 56.86823245872843"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 29065.834318033852,
            "unit": "ns",
            "range": "± 112.80837485889612"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 36431.05161946615,
            "unit": "ns",
            "range": "± 222.17969613379665"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 33484.525026448566,
            "unit": "ns",
            "range": "± 172.01984373546426"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 15977.89966430664,
            "unit": "ns",
            "range": "± 64.0520733434949"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19867.111700204703,
            "unit": "ns",
            "range": "± 13.508046009941243"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 21547.40353863056,
            "unit": "ns",
            "range": "± 12.558472542123747"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 23209.277825927733,
            "unit": "ns",
            "range": "± 110.45306296028286"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 16714.808764139812,
            "unit": "ns",
            "range": "± 10.328873925025462"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10411.49609048026,
            "unit": "ns",
            "range": "± 64.69986225187931"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 21990.21354370117,
            "unit": "ns",
            "range": "± 115.93855209096581"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 21747.044090270996,
            "unit": "ns",
            "range": "± 25.603235580847386"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 27202.476776123047,
            "unit": "ns",
            "range": "± 76.05060710802717"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 27975.65589396159,
            "unit": "ns",
            "range": "± 81.90009165621437"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675762854,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 31631.47147042411,
            "unit": "ns",
            "range": "± 53.912955786073134"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 47079.1268484933,
            "unit": "ns",
            "range": "± 66.145510624909"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 64373.688151041664,
            "unit": "ns",
            "range": "± 116.7915986301994"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 49552.30407714844,
            "unit": "ns",
            "range": "± 92.82719765688765"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 17796.890970865887,
            "unit": "ns",
            "range": "± 25.504293237249477"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 26824.551696777344,
            "unit": "ns",
            "range": "± 61.536108318940705"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 134668.65234375,
            "unit": "ns",
            "range": "± 330.4233920166886"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 221970.6308218149,
            "unit": "ns",
            "range": "± 314.37338844670313"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 31405.0288609096,
            "unit": "ns",
            "range": "± 80.38731428228215"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 54274.307861328125,
            "unit": "ns",
            "range": "± 111.12064365822069"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 71425.11549729567,
            "unit": "ns",
            "range": "± 212.65691695057254"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 50016.81126185826,
            "unit": "ns",
            "range": "± 73.8670577014536"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15406.226642315205,
            "unit": "ns",
            "range": "± 12.229286369183129"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 32153.777262369793,
            "unit": "ns",
            "range": "± 66.70707208256694"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 133387.5040690104,
            "unit": "ns",
            "range": "± 288.0126195781143"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 238020.37434895834,
            "unit": "ns",
            "range": "± 1327.3778553824145"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 30951.822713216145,
            "unit": "ns",
            "range": "± 62.53324054157383"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 49751.588657924105,
            "unit": "ns",
            "range": "± 25.574476777483195"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 64577.01275165264,
            "unit": "ns",
            "range": "± 59.013719719869755"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 49740.97028459822,
            "unit": "ns",
            "range": "± 93.56375025105173"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 18170.255737304688,
            "unit": "ns",
            "range": "± 36.330141408542126"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 25736.04255089393,
            "unit": "ns",
            "range": "± 44.747426138992545"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 140245.4386393229,
            "unit": "ns",
            "range": "± 424.6941989640478"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 227548.4903971354,
            "unit": "ns",
            "range": "± 686.9577111832157"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675770928,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 14683.917170206705,
            "unit": "ns",
            "range": "± 16.504380271575197"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 18425.73007420131,
            "unit": "ns",
            "range": "± 14.436436937535124"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 21495.389545147234,
            "unit": "ns",
            "range": "± 112.5158911145005"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 21160.251127115884,
            "unit": "ns",
            "range": "± 182.4273461411999"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 16454.694485982258,
            "unit": "ns",
            "range": "± 25.393999841718475"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 9914.123772254357,
            "unit": "ns",
            "range": "± 24.34287868371558"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 20839.64963684082,
            "unit": "ns",
            "range": "± 159.04778549815916"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 22077.815358479816,
            "unit": "ns",
            "range": "± 28.176601034014425"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 27570.266844685873,
            "unit": "ns",
            "range": "± 139.26968292989505"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 27655.50278116862,
            "unit": "ns",
            "range": "± 97.92965581647522"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 21350.126892089844,
            "unit": "ns",
            "range": "± 51.2942953329386"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26012.6969523112,
            "unit": "ns",
            "range": "± 113.01771582267368"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 27775.585290091378,
            "unit": "ns",
            "range": "± 128.23692307505138"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 29952.331869942802,
            "unit": "ns",
            "range": "± 235.1876833375551"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 17055.36888885498,
            "unit": "ns",
            "range": "± 6.742346396602698"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10191.995281473795,
            "unit": "ns",
            "range": "± 60.96459999328973"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 27500.176534016926,
            "unit": "ns",
            "range": "± 125.9064014815396"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 27217.15358581543,
            "unit": "ns",
            "range": "± 122.04738976529501"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 31633.650541178384,
            "unit": "ns",
            "range": "± 142.8045757408758"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 31966.22900390625,
            "unit": "ns",
            "range": "± 272.3102354573847"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14238.281033833822,
            "unit": "ns",
            "range": "± 69.88748447034718"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19210.96667597844,
            "unit": "ns",
            "range": "± 22.695006579036495"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 20002.26869553786,
            "unit": "ns",
            "range": "± 29.80848127765747"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 22480.55998665946,
            "unit": "ns",
            "range": "± 111.06940684290194"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15190.376298170824,
            "unit": "ns",
            "range": "± 73.06483668580645"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10342.702044169107,
            "unit": "ns",
            "range": "± 63.4137340377722"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 22010.59789276123,
            "unit": "ns",
            "range": "± 13.798981184811682"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 22013.198693847655,
            "unit": "ns",
            "range": "± 146.2036617658359"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 28143.010638427735,
            "unit": "ns",
            "range": "± 94.24662815378727"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 27172.012858072918,
            "unit": "ns",
            "range": "± 120.80324036252733"
          }
        ]
      }
    ],
    "Operations.ModuleOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744675785796,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 34356.39953613281,
            "unit": "ns",
            "range": "± 304.2943234514203"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 48914.05770438058,
            "unit": "ns",
            "range": "± 77.87769950262876"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 70916.97736467634,
            "unit": "ns",
            "range": "± 123.4896513865784"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 55267.95741489955,
            "unit": "ns",
            "range": "± 95.21646401517306"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 17721.18639264788,
            "unit": "ns",
            "range": "± 21.5901054609683"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 28125.7567185622,
            "unit": "ns",
            "range": "± 69.45436843132704"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 157104.34744698662,
            "unit": "ns",
            "range": "± 193.67685468710027"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 281455.50255408656,
            "unit": "ns",
            "range": "± 1019.0862419392146"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 32026.76522391183,
            "unit": "ns",
            "range": "± 27.90717450126384"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 54995.454915364586,
            "unit": "ns",
            "range": "± 102.3851514664057"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 78248.41837565105,
            "unit": "ns",
            "range": "± 344.05505065959517"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 53991.59720284598,
            "unit": "ns",
            "range": "± 48.135567539313804"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 17788.301438551684,
            "unit": "ns",
            "range": "± 15.855997538008564"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 35663.236490885414,
            "unit": "ns",
            "range": "± 170.7118214319455"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 168722.2149188702,
            "unit": "ns",
            "range": "± 291.9308627507889"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 287840.5094401042,
            "unit": "ns",
            "range": "± 1268.0903891050052"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 33209.43013509115,
            "unit": "ns",
            "range": "± 33.540771289996265"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 49528.7978108724,
            "unit": "ns",
            "range": "± 54.88480526335429"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 74271.9364420573,
            "unit": "ns",
            "range": "± 299.23221794968816"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 53281.21032714844,
            "unit": "ns",
            "range": "± 111.1065617799902"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 17529.18513371394,
            "unit": "ns",
            "range": "± 9.60346641958998"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 30381.18685208834,
            "unit": "ns",
            "range": "± 69.93384897392886"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 152228.29752604166,
            "unit": "ns",
            "range": "± 673.7651935810194"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 280946.62109375,
            "unit": "ns",
            "range": "± 957.0482826074066"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744675904494,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 140824.96514020648,
            "unit": "ns",
            "range": "± 693.5017573126012"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 19043.455439976282,
            "unit": "ns",
            "range": "± 87.70300686822434"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 16716.786825326773,
            "unit": "ns",
            "range": "± 9.914267432000935"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 144269.02366129556,
            "unit": "ns",
            "range": "± 373.57795584427686"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 46014.53926188151,
            "unit": "ns",
            "range": "± 279.58978885888644"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 130692.9629720052,
            "unit": "ns",
            "range": "± 187.95906542455506"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 10577703.95200893,
            "unit": "ns",
            "range": "± 111834.13756030884"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 285807.1913599918,
            "unit": "ns",
            "range": "± 16368.312555531209"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 140160.3995279948,
            "unit": "ns",
            "range": "± 756.8175016852975"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 18792.32160441081,
            "unit": "ns",
            "range": "± 114.49119969280538"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 16845.710935465493,
            "unit": "ns",
            "range": "± 118.37232995241045"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 145100.314461263,
            "unit": "ns",
            "range": "± 633.1784380911857"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 45415.275866699216,
            "unit": "ns",
            "range": "± 194.2166895459824"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 133651.71385091144,
            "unit": "ns",
            "range": "± 735.979821468506"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 10577631.697916666,
            "unit": "ns",
            "range": "± 167157.1482108968"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 278253.46101668075,
            "unit": "ns",
            "range": "± 13885.003220554507"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 141035.67483956474,
            "unit": "ns",
            "range": "± 1264.4746848959885"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 18615.545194185695,
            "unit": "ns",
            "range": "± 52.64032044048708"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 16707.82697237455,
            "unit": "ns",
            "range": "± 17.124691670573295"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 143590.4295247396,
            "unit": "ns",
            "range": "± 1154.6373481028652"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 45998.51728703426,
            "unit": "ns",
            "range": "± 14.042071938255717"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 129169.825,
            "unit": "ns",
            "range": "± 752.6330788531569"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 8663425.021205356,
            "unit": "ns",
            "range": "± 79505.53102070438"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 253274.56790865384,
            "unit": "ns",
            "range": "± 476.5988541274249"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 140079.3938860212,
            "unit": "ns",
            "range": "± 842.8720078119543"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 18552.628039550782,
            "unit": "ns",
            "range": "± 107.67444701682192"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 17123.365951538086,
            "unit": "ns",
            "range": "± 122.82249845980971"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 145120.89514973958,
            "unit": "ns",
            "range": "± 1034.2655544240802"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 45465.08370361328,
            "unit": "ns",
            "range": "± 202.15396064965898"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 127295.13539632161,
            "unit": "ns",
            "range": "± 154.68142899254826"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 9749207.813701924,
            "unit": "ns",
            "range": "± 42157.10814071842"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 282116.45284598216,
            "unit": "ns",
            "range": "± 1436.232021146904"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 138368.89744215744,
            "unit": "ns",
            "range": "± 373.7329162790428"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 18857.35508626302,
            "unit": "ns",
            "range": "± 107.29948668040747"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 16698.16166334886,
            "unit": "ns",
            "range": "± 23.645195381209568"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 144526.22439778646,
            "unit": "ns",
            "range": "± 196.99328459466884"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 45479.9570332845,
            "unit": "ns",
            "range": "± 185.14660016208774"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 126484.95714393028,
            "unit": "ns",
            "range": "± 467.3931718732164"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 9559731.76875,
            "unit": "ns",
            "range": "± 90673.9826410806"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 279664.70857747394,
            "unit": "ns",
            "range": "± 509.95156043632795"
          }
        ]
      }
    ]
  }
}