window.BENCHMARK_DATA = {
  "lastUpdate": 1744675429327,
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
    ]
  }
}