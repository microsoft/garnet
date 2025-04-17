window.BENCHMARK_DATA = {
  "lastUpdate": 1744899091748,
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769542004,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 85.17183752059937,
            "unit": "ns",
            "range": "± 0.655778715364176"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898694381,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 83.68836737076441,
            "unit": "ns",
            "range": "± 0.9419038299027352"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769535583,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 89.65643615722657,
            "unit": "ns",
            "range": "± 1.238147921746852"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898691096,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 87.62434962621101,
            "unit": "ns",
            "range": "± 0.934322090133866"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769562085,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3836.723404255319,
            "unit": "ns",
            "range": "± 509.01537155434596"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3669.3186813186812,
            "unit": "ns",
            "range": "± 642.7541240478076"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 412874.7173913043,
            "unit": "ns",
            "range": "± 10214.31208895601"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 362107.7525252525,
            "unit": "ns",
            "range": "± 46118.44548425889"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 17511.337078651686,
            "unit": "ns",
            "range": "± 2390.6907424415886"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 135741.2105263158,
            "unit": "ns",
            "range": "± 6771.2435476620085"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3565.2849462365593,
            "unit": "ns",
            "range": "± 680.9967574694938"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 4942.675824175824,
            "unit": "ns",
            "range": "± 1050.4561082734658"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 365350.5606060606,
            "unit": "ns",
            "range": "± 44621.90556030776"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 386308.6632653061,
            "unit": "ns",
            "range": "± 63643.31496594527"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 18253.56818181818,
            "unit": "ns",
            "range": "± 3891.4633503082173"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 148017.85353535353,
            "unit": "ns",
            "range": "± 24308.1585196953"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3711.465909090909,
            "unit": "ns",
            "range": "± 994.1133743810622"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 5092.212765957447,
            "unit": "ns",
            "range": "± 1634.1293684875618"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 336420.5540540541,
            "unit": "ns",
            "range": "± 16898.38870177321"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 332206.31746031746,
            "unit": "ns",
            "range": "± 12974.682755554017"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 15686.734042553191,
            "unit": "ns",
            "range": "± 2159.4570127225006"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 157298.0612244898,
            "unit": "ns",
            "range": "± 24032.34486204744"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3438.9270833333335,
            "unit": "ns",
            "range": "± 596.9030732352776"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3742.296703296703,
            "unit": "ns",
            "range": "± 451.4193294366237"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 434997.1842105263,
            "unit": "ns",
            "range": "± 15000.689535241934"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 442341.8101265823,
            "unit": "ns",
            "range": "± 22595.196915907018"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 26251.782608695652,
            "unit": "ns",
            "range": "± 5684.068610002056"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 167432.33333333334,
            "unit": "ns",
            "range": "± 21177.21476037976"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 5695.489583333333,
            "unit": "ns",
            "range": "± 2454.353695588361"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3641.904255319149,
            "unit": "ns",
            "range": "± 568.1209561721099"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 487108.26,
            "unit": "ns",
            "range": "± 97789.89737079336"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 435252.0737704918,
            "unit": "ns",
            "range": "± 19573.262692985907"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 16153.653846153846,
            "unit": "ns",
            "range": "± 271.1124754764604"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 175653.05102040817,
            "unit": "ns",
            "range": "± 35247.894783482785"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898728373,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 4998.865979381443,
            "unit": "ns",
            "range": "± 2382.539819660532"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 8091.163265306122,
            "unit": "ns",
            "range": "± 4205.128149839763"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 377964.0360824742,
            "unit": "ns",
            "range": "± 33338.17450419447"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 379944.27894736844,
            "unit": "ns",
            "range": "± 36578.69299753701"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 22583.287234042553,
            "unit": "ns",
            "range": "± 5858.176045314769"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 161660.32291666666,
            "unit": "ns",
            "range": "± 26891.233709732798"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 5866.09375,
            "unit": "ns",
            "range": "± 2416.371562479688"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 5826.551724137931,
            "unit": "ns",
            "range": "± 2006.2720186931729"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 384153.59278350516,
            "unit": "ns",
            "range": "± 44616.78006859323"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 394277.43157894735,
            "unit": "ns",
            "range": "± 58671.65839212294"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 21164.152173913044,
            "unit": "ns",
            "range": "± 4957.140235785949"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 160658.74489795917,
            "unit": "ns",
            "range": "± 22669.262045143103"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 6657.1875,
            "unit": "ns",
            "range": "± 2603.5280246235106"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 5822.9639175257735,
            "unit": "ns",
            "range": "± 2868.8115718223794"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 345150.4561403509,
            "unit": "ns",
            "range": "± 14841.01038997559"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 343877.8333333333,
            "unit": "ns",
            "range": "± 22076.10501957009"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 21322.74226804124,
            "unit": "ns",
            "range": "± 4935.755109990667"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 161085.67708333334,
            "unit": "ns",
            "range": "± 25080.849085977425"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 6493.432989690722,
            "unit": "ns",
            "range": "± 2483.9596787321266"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 6945.613402061856,
            "unit": "ns",
            "range": "± 3915.9178678086028"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 449561.60493827163,
            "unit": "ns",
            "range": "± 23504.552197754696"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 479778.59574468085,
            "unit": "ns",
            "range": "± 42617.3507997583"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 25484.463917525773,
            "unit": "ns",
            "range": "± 5708.797155235244"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 142255.66666666666,
            "unit": "ns",
            "range": "± 2214.342600302661"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 6059.229166666667,
            "unit": "ns",
            "range": "± 2978.788731149721"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 7368.666666666667,
            "unit": "ns",
            "range": "± 3928.7245113031627"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 451559.925,
            "unit": "ns",
            "range": "± 16059.711352081627"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 509664.43617021275,
            "unit": "ns",
            "range": "± 42969.95044590857"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 24816.35789473684,
            "unit": "ns",
            "range": "± 6948.330393233143"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 171902.45360824742,
            "unit": "ns",
            "range": "± 28115.268614602683"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769570938,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 137421.70457240514,
            "unit": "ns",
            "range": "± 839.8677776725367"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 160428.26458333334,
            "unit": "ns",
            "range": "± 921.9105359250794"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 135772.5142252604,
            "unit": "ns",
            "range": "± 513.9787783245368"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898722560,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 136371.73439378006,
            "unit": "ns",
            "range": "± 770.7864845842078"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 150921.72023925782,
            "unit": "ns",
            "range": "± 810.948741605946"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 136859.3618861607,
            "unit": "ns",
            "range": "± 260.0455465064902"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769550323,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3302.0105263157893,
            "unit": "ns",
            "range": "± 447.824112859313"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3838.95,
            "unit": "ns",
            "range": "± 149.708939835491"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 438708.1666666667,
            "unit": "ns",
            "range": "± 11222.587397033625"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 358307.488372093,
            "unit": "ns",
            "range": "± 25336.26093716935"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 17825.73404255319,
            "unit": "ns",
            "range": "± 3205.655239499027"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 155457.93434343435,
            "unit": "ns",
            "range": "± 19561.4519223247"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3645.3956043956046,
            "unit": "ns",
            "range": "± 424.5507921221855"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3142.6382978723404,
            "unit": "ns",
            "range": "± 415.23023412885783"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 450371.5,
            "unit": "ns",
            "range": "± 13462.899218298744"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 426081.16161616164,
            "unit": "ns",
            "range": "± 61185.17398876004"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 18553.68085106383,
            "unit": "ns",
            "range": "± 3508.454121373103"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 165585.78787878787,
            "unit": "ns",
            "range": "± 23790.84313227132"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3568.121052631579,
            "unit": "ns",
            "range": "± 354.5315812523098"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 3637.4574468085107,
            "unit": "ns",
            "range": "± 424.1591724177367"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 339332,
            "unit": "ns",
            "range": "± 5600.746485273878"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 347872.10714285716,
            "unit": "ns",
            "range": "± 9811.471537380925"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 15539.105263157895,
            "unit": "ns",
            "range": "± 1619.017678754911"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 138960.66666666666,
            "unit": "ns",
            "range": "± 2127.7131912554437"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3572.735294117647,
            "unit": "ns",
            "range": "± 80.61678594728636"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3667.3396226415093,
            "unit": "ns",
            "range": "± 175.94874072124904"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 461436,
            "unit": "ns",
            "range": "± 10301.637636803189"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 470377.2068965517,
            "unit": "ns",
            "range": "± 13572.712139171295"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 20163.6875,
            "unit": "ns",
            "range": "± 1888.9980653217922"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 161433.7319587629,
            "unit": "ns",
            "range": "± 26031.025182108602"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3389.285714285714,
            "unit": "ns",
            "range": "± 65.97485368442523"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3516.3645833333335,
            "unit": "ns",
            "range": "± 516.3938240038295"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 457406.4054054054,
            "unit": "ns",
            "range": "± 15486.563813439952"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 510226.98,
            "unit": "ns",
            "range": "± 99852.8006030493"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 19610.790322580644,
            "unit": "ns",
            "range": "± 2156.0279379322665"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 154394.25531914894,
            "unit": "ns",
            "range": "± 17615.353530791155"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898709118,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 3041.2842105263157,
            "unit": "ns",
            "range": "± 374.61897417492105"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 3159.2291666666665,
            "unit": "ns",
            "range": "± 467.2581948067204"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 436736.23333333334,
            "unit": "ns",
            "range": "± 12705.117089347228"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 441636.6666666667,
            "unit": "ns",
            "range": "± 11371.079841141876"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 17574.594736842104,
            "unit": "ns",
            "range": "± 2888.0166612418116"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 149291.11111111112,
            "unit": "ns",
            "range": "± 15056.55747973585"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 3118.4270833333335,
            "unit": "ns",
            "range": "± 447.53285665422123"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 3250.766666666667,
            "unit": "ns",
            "range": "± 42.905988038079364"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 456767.1818181818,
            "unit": "ns",
            "range": "± 11192.137625674313"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 463824.44444444444,
            "unit": "ns",
            "range": "± 9817.456010909536"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 17807.094736842104,
            "unit": "ns",
            "range": "± 3450.739661917964"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 162809.83673469388,
            "unit": "ns",
            "range": "± 21996.975202060847"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 3695.1979166666665,
            "unit": "ns",
            "range": "± 311.03039688282513"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 3886.777777777778,
            "unit": "ns",
            "range": "± 159.03728677847403"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 350676.3777777778,
            "unit": "ns",
            "range": "± 13110.583043495968"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 337069.8461538461,
            "unit": "ns",
            "range": "± 3033.7790747447275"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 14720.935483870968,
            "unit": "ns",
            "range": "± 1192.1372824218915"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 152794.75,
            "unit": "ns",
            "range": "± 15761.731873240942"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 3220.0714285714284,
            "unit": "ns",
            "range": "± 63.37612300411699"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 3889.2615384615383,
            "unit": "ns",
            "range": "± 198.23930274757865"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 546483.18,
            "unit": "ns",
            "range": "± 104485.9434187618"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 447375.72602739726,
            "unit": "ns",
            "range": "± 18346.406121923803"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 20390.75,
            "unit": "ns",
            "range": "± 1809.9936107288115"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 142248.58333333334,
            "unit": "ns",
            "range": "± 1799.2883167990885"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 3734.2365591397847,
            "unit": "ns",
            "range": "± 294.8920730298763"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 3617,
            "unit": "ns",
            "range": "± 67.82856541099986"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 451690.3870967742,
            "unit": "ns",
            "range": "± 13653.335642929704"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 451840.01851851854,
            "unit": "ns",
            "range": "± 12439.904861033583"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 19588.340425531915,
            "unit": "ns",
            "range": "± 1664.7482198676485"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 158136.6616161616,
            "unit": "ns",
            "range": "± 15972.009933009193"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769569505,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 18149.118204752605,
            "unit": "ns",
            "range": "± 69.57387277993496"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 17700.462533804086,
            "unit": "ns",
            "range": "± 15.56834513484182"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 17384.749873570032,
            "unit": "ns",
            "range": "± 10.452776848364499"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898725222,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 17766.362060546875,
            "unit": "ns",
            "range": "± 66.10308211263118"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 18721.12355041504,
            "unit": "ns",
            "range": "± 32.56473114074465"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 17523.037123460035,
            "unit": "ns",
            "range": "± 15.56285242522118"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769566137,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1252.0777777777778,
            "unit": "ns",
            "range": "± 552.3187531838854"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 970.6979166666666,
            "unit": "ns",
            "range": "± 309.0403863844767"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 2028.0315789473684,
            "unit": "ns",
            "range": "± 507.23146029333617"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 479875.94736842107,
            "unit": "ns",
            "range": "± 10614.61261905641"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 2212.6129032258063,
            "unit": "ns",
            "range": "± 528.6624021848642"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 11916.953608247422,
            "unit": "ns",
            "range": "± 1992.1592819341079"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1448.5157894736842,
            "unit": "ns",
            "range": "± 619.8649108691051"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 1069.4479166666667,
            "unit": "ns",
            "range": "± 399.7896781214545"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 2169.564516129032,
            "unit": "ns",
            "range": "± 429.46963769918574"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 497648.98,
            "unit": "ns",
            "range": "± 71010.28134457686"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 2111.1684210526314,
            "unit": "ns",
            "range": "± 644.6751062428132"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 10894.709677419354,
            "unit": "ns",
            "range": "± 1612.6048640027502"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1405.4166666666667,
            "unit": "ns",
            "range": "± 561.6892129370337"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 976.9263157894737,
            "unit": "ns",
            "range": "± 337.12704348516473"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1811.1612903225807,
            "unit": "ns",
            "range": "± 605.3604260618588"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 408507.96153846156,
            "unit": "ns",
            "range": "± 11015.18783673077"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 2963.577319587629,
            "unit": "ns",
            "range": "± 1524.0360808492167"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 11754.833333333334,
            "unit": "ns",
            "range": "± 2281.588154607745"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1308.042105263158,
            "unit": "ns",
            "range": "± 604.1337198003235"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 679.5860215053764,
            "unit": "ns",
            "range": "± 328.5658777142417"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1950.3695652173913,
            "unit": "ns",
            "range": "± 764.5521308725697"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 542162.21,
            "unit": "ns",
            "range": "± 101074.95390448534"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 2201.6770833333335,
            "unit": "ns",
            "range": "± 416.5879573465134"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 10791.45054945055,
            "unit": "ns",
            "range": "± 860.5552634295842"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1191.8157894736842,
            "unit": "ns",
            "range": "± 525.9762405271783"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 786.7684210526315,
            "unit": "ns",
            "range": "± 350.0246937377473"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 2127.3526315789472,
            "unit": "ns",
            "range": "± 580.3754465021549"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 486712.9,
            "unit": "ns",
            "range": "± 14137.74762178337"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 2178.778947368421,
            "unit": "ns",
            "range": "± 528.353407253447"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 12442.613402061856,
            "unit": "ns",
            "range": "± 2801.377685161835"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898720076,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1553.2307692307693,
            "unit": "ns",
            "range": "± 27.486826681624066"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 904.680412371134,
            "unit": "ns",
            "range": "± 367.57138043718095"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1410.5666666666666,
            "unit": "ns",
            "range": "± 34.707896727374525"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 473099.63636363635,
            "unit": "ns",
            "range": "± 69607.76509030022"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1586.095744680851,
            "unit": "ns",
            "range": "± 349.69536109747787"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 8752.4,
            "unit": "ns",
            "range": "± 169.38367268254802"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1013.8152173913044,
            "unit": "ns",
            "range": "± 321.0247304618198"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 904.7731958762887,
            "unit": "ns",
            "range": "± 325.1402103053209"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1317.0736842105264,
            "unit": "ns",
            "range": "± 619.1331662022327"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 483683.14285714284,
            "unit": "ns",
            "range": "± 7980.497679267328"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 1685.9072164948454,
            "unit": "ns",
            "range": "± 455.97140084097356"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 8451.54347826087,
            "unit": "ns",
            "range": "± 414.014010660289"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1341.878947368421,
            "unit": "ns",
            "range": "± 415.0790793394231"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 1016.360824742268,
            "unit": "ns",
            "range": "± 306.92361921599655"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1532.8369565217392,
            "unit": "ns",
            "range": "± 299.00767299405004"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 408194.73076923075,
            "unit": "ns",
            "range": "± 10819.290891949222"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 1955.465909090909,
            "unit": "ns",
            "range": "± 368.52601551560474"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 8827.385057471265,
            "unit": "ns",
            "range": "± 1116.6040024388667"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1256.0104166666667,
            "unit": "ns",
            "range": "± 401.82877984460424"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 980.3958333333334,
            "unit": "ns",
            "range": "± 383.3140111453675"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1686.3315789473684,
            "unit": "ns",
            "range": "± 512.2156222461515"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 480457.86666666664,
            "unit": "ns",
            "range": "± 14114.782555969427"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1651.5208333333333,
            "unit": "ns",
            "range": "± 386.5119043354068"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 8606.891304347826,
            "unit": "ns",
            "range": "± 1212.6377747170866"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1044.9479166666667,
            "unit": "ns",
            "range": "± 544.9531775114486"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 773.8247422680413,
            "unit": "ns",
            "range": "± 332.7152501986896"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 1467.95,
            "unit": "ns",
            "range": "± 41.69750087172334"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 485059.0714285714,
            "unit": "ns",
            "range": "± 6437.854569345595"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 1425,
            "unit": "ns",
            "range": "± 43.575877992957786"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 8945.917808219177,
            "unit": "ns",
            "range": "± 462.0407555804484"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769563462,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 18261.523419893703,
            "unit": "ns",
            "range": "± 13.859856952478884"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 18922.517920939128,
            "unit": "ns",
            "range": "± 197.1157005725388"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 18226.669254847937,
            "unit": "ns",
            "range": "± 64.48506671794456"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898724661,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 19208.76983388265,
            "unit": "ns",
            "range": "± 16.618600292236927"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 18031.822863260906,
            "unit": "ns",
            "range": "± 13.93434381235257"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 18197.122614933895,
            "unit": "ns",
            "range": "± 11.631105922506514"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769575417,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 36491.370666503906,
            "unit": "ns",
            "range": "± 83.40120710813441"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 39833.9565386091,
            "unit": "ns",
            "range": "± 36.66051637726394"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 31936.65879313151,
            "unit": "ns",
            "range": "± 51.386554507481684"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 31612.83539254325,
            "unit": "ns",
            "range": "± 144.43096218847649"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898747332,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 36734.82635732798,
            "unit": "ns",
            "range": "± 53.30947115958562"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 40120.10790812175,
            "unit": "ns",
            "range": "± 243.13979100979762"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 32130.796677809496,
            "unit": "ns",
            "range": "± 26.741626373295652"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 33743.99947916667,
            "unit": "ns",
            "range": "± 155.56886092060313"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769592182,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1658.3083478382655,
            "unit": "ns",
            "range": "± 8.872705292019472"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1632.661193974813,
            "unit": "ns",
            "range": "± 16.08688128700809"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1683.3876425879341,
            "unit": "ns",
            "range": "± 7.792531323505918"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898742466,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1682.4814225605555,
            "unit": "ns",
            "range": "± 6.954756354031385"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1622.4815965016683,
            "unit": "ns",
            "range": "± 8.65886273478604"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1692.2411490122477,
            "unit": "ns",
            "range": "± 14.736710019642997"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769593154,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 101670.73323567708,
            "unit": "ns",
            "range": "± 246.67220739097326"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 119393.78307233538,
            "unit": "ns",
            "range": "± 497.0600408692881"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 99163.63738141741,
            "unit": "ns",
            "range": "± 739.8051732709296"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898749934,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 104628.14764811198,
            "unit": "ns",
            "range": "± 1317.4734899356786"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 113043.52080078125,
            "unit": "ns",
            "range": "± 475.9111280131679"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 102645.32159423828,
            "unit": "ns",
            "range": "± 559.5920608548096"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769619473,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1786.4526615142822,
            "unit": "ns",
            "range": "± 17.623314241912226"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1777.962025642395,
            "unit": "ns",
            "range": "± 8.981191623142596"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1748.8785008748373,
            "unit": "ns",
            "range": "± 11.153903339692556"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898747585,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1633.805403489333,
            "unit": "ns",
            "range": "± 13.625375289982022"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1599.921380106608,
            "unit": "ns",
            "range": "± 13.951534784359389"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1563.3769484202066,
            "unit": "ns",
            "range": "± 12.724756308202391"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769579339,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 34767.85596720377,
            "unit": "ns",
            "range": "± 31.710872178500008"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 36961.51287841797,
            "unit": "ns",
            "range": "± 150.11610600981047"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 31778.94241841634,
            "unit": "ns",
            "range": "± 45.71828735144689"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30578.424961635046,
            "unit": "ns",
            "range": "± 126.7888128591345"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898755061,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 35715.024213518416,
            "unit": "ns",
            "range": "± 56.12837637272552"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 37725.8993733724,
            "unit": "ns",
            "range": "± 153.9357549970597"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 31232.41960449219,
            "unit": "ns",
            "range": "± 158.22087319436656"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30320.507066999162,
            "unit": "ns",
            "range": "± 219.30509473373647"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769561770,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1129.1701030927834,
            "unit": "ns",
            "range": "± 436.2668984704557"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 732.1701030927835,
            "unit": "ns",
            "range": "± 365.14054718838184"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1592.8279569892472,
            "unit": "ns",
            "range": "± 310.97415744916094"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 409375.40625,
            "unit": "ns",
            "range": "± 43995.74612967608"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1498.6770833333333,
            "unit": "ns",
            "range": "± 691.4531151254338"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 8409.106382978724,
            "unit": "ns",
            "range": "± 704.3758232569647"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 936.8645833333334,
            "unit": "ns",
            "range": "± 509.2040356186809"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 670.9583333333334,
            "unit": "ns",
            "range": "± 411.61957184981463"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1613.9,
            "unit": "ns",
            "range": "± 427.31941314555814"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 391060.1978021978,
            "unit": "ns",
            "range": "± 28464.96927422656"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 1608.1516853932585,
            "unit": "ns",
            "range": "± 268.9941322916812"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 8033.645833333333,
            "unit": "ns",
            "range": "± 793.8525115458913"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 898.2613636363636,
            "unit": "ns",
            "range": "± 249.0717707726435"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 716.3225806451613,
            "unit": "ns",
            "range": "± 244.5928809662187"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1534.8505154639174,
            "unit": "ns",
            "range": "± 417.89649439277804"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 379482.1857142857,
            "unit": "ns",
            "range": "± 18354.446545367402"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 1703.5833333333333,
            "unit": "ns",
            "range": "± 457.17196273259384"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 7217.75,
            "unit": "ns",
            "range": "± 97.61065981280379"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 799.5515463917526,
            "unit": "ns",
            "range": "± 566.8140195266512"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 766.7340425531914,
            "unit": "ns",
            "range": "± 284.68708252689584"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1581.6666666666667,
            "unit": "ns",
            "range": "± 315.8694795162831"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 426443.98,
            "unit": "ns",
            "range": "± 16805.328340203316"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1486.2659574468084,
            "unit": "ns",
            "range": "± 617.9614278865025"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 7780.214285714285,
            "unit": "ns",
            "range": "± 138.07881170461397"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 926.9368421052632,
            "unit": "ns",
            "range": "± 398.91802630781905"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 714.0416666666666,
            "unit": "ns",
            "range": "± 352.9140022476527"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 1626.0824742268042,
            "unit": "ns",
            "range": "± 354.76619854088096"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 425792.9918032787,
            "unit": "ns",
            "range": "± 19197.45058649104"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 1822.8041237113403,
            "unit": "ns",
            "range": "± 421.5530966748449"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 7768.2947368421055,
            "unit": "ns",
            "range": "± 927.3890926829598"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898721052,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1130.6134020618556,
            "unit": "ns",
            "range": "± 455.5829570517468"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 746.1082474226804,
            "unit": "ns",
            "range": "± 533.9381900225951"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1647.9791666666667,
            "unit": "ns",
            "range": "± 746.6372892286231"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 414087.4065934066,
            "unit": "ns",
            "range": "± 42054.947197149915"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1618.659574468085,
            "unit": "ns",
            "range": "± 698.4078445098789"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 8609.53125,
            "unit": "ns",
            "range": "± 888.8747700930063"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1017.6030927835052,
            "unit": "ns",
            "range": "± 514.815267930645"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 815.8814432989691,
            "unit": "ns",
            "range": "± 475.8556206827816"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1634.1546391752577,
            "unit": "ns",
            "range": "± 471.6190937479406"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 438727.5104166667,
            "unit": "ns",
            "range": "± 60644.471346398095"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 1616.8494623655913,
            "unit": "ns",
            "range": "± 662.4038887828995"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 10691.065217391304,
            "unit": "ns",
            "range": "± 1575.0239684507842"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1089.3548387096773,
            "unit": "ns",
            "range": "± 672.3450952217349"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 899.4484536082474,
            "unit": "ns",
            "range": "± 437.25679838277966"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1751.6976744186047,
            "unit": "ns",
            "range": "± 757.4060542756122"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 388181.2083333333,
            "unit": "ns",
            "range": "± 9951.110359627852"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 1967.21875,
            "unit": "ns",
            "range": "± 465.8486585763325"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 8584.106382978724,
            "unit": "ns",
            "range": "± 1048.8592930026216"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1087.9270833333333,
            "unit": "ns",
            "range": "± 693.7724675514704"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 697.4137931034483,
            "unit": "ns",
            "range": "± 338.3807732636635"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1721.7604166666667,
            "unit": "ns",
            "range": "± 546.5025977651297"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 460094.6666666667,
            "unit": "ns",
            "range": "± 15803.518945763493"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1721.8052631578948,
            "unit": "ns",
            "range": "± 716.0826214318325"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 8918.734042553191,
            "unit": "ns",
            "range": "± 1239.9149591505934"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1125.041237113402,
            "unit": "ns",
            "range": "± 434.9833549862801"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 732.0631578947368,
            "unit": "ns",
            "range": "± 511.09073621960323"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 1695.1546391752577,
            "unit": "ns",
            "range": "± 462.802881459946"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 451396.4285714286,
            "unit": "ns",
            "range": "± 6081.357559273773"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 1818.1521739130435,
            "unit": "ns",
            "range": "± 378.18621306634634"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 9276.466666666667,
            "unit": "ns",
            "range": "± 770.1993095326003"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769630487,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 83.03098848887852,
            "unit": "ns",
            "range": "± 0.11976200932929001"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898780065,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 81.50566305433001,
            "unit": "ns",
            "range": "± 0.054862036823409316"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769624180,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 76.33569581168038,
            "unit": "ns",
            "range": "± 0.10388139569599295"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898828378,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.BasicOperations.InlinePing(Params: None)",
            "value": 70.21376353043776,
            "unit": "ns",
            "range": "± 0.171234341344957"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769713834,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 34707.49065692608,
            "unit": "ns",
            "range": "± 42.17107507203952"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 34562.51656668527,
            "unit": "ns",
            "range": "± 29.325915217379706"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 30968.10326209435,
            "unit": "ns",
            "range": "± 31.160539648645493"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 29076.644427959734,
            "unit": "ns",
            "range": "± 35.702749175871446"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769737981,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15360.592827430139,
            "unit": "ns",
            "range": "± 9.754251881672268"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15293.786185128349,
            "unit": "ns",
            "range": "± 13.750994630454185"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 15189.930725097656,
            "unit": "ns",
            "range": "± 10.999406361112166"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898865459,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15770.262756347656,
            "unit": "ns",
            "range": "± 27.697292123005028"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15842.670288085938,
            "unit": "ns",
            "range": "± 33.176029878544405"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 15554.349568684896,
            "unit": "ns",
            "range": "± 30.47250842981389"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769843906,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1741.625199999128,
            "unit": "ns",
            "range": "± 2.797513498466203"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1672.1200370788574,
            "unit": "ns",
            "range": "± 1.1868141841533277"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1670.5930196321929,
            "unit": "ns",
            "range": "± 1.5216026549533528"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769745310,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 40080.851091657365,
            "unit": "ns",
            "range": "± 109.37289620061198"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 40748.3388264974,
            "unit": "ns",
            "range": "± 305.16084336406163"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 36539.134114583336,
            "unit": "ns",
            "range": "± 133.82865505820314"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 34685.39167131697,
            "unit": "ns",
            "range": "± 181.4471050974347"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898882680,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Get(Params: None)",
            "value": 35271.51916503906,
            "unit": "ns",
            "range": "± 30.579743307056834"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.Set(Params: None)",
            "value": 36484.84598795573,
            "unit": "ns",
            "range": "± 35.76437922418703"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MGet(Params: None)",
            "value": 30858.11767578125,
            "unit": "ns",
            "range": "± 26.074279415815067"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterMigrate.MSet(Params: None)",
            "value": 30346.03271484375,
            "unit": "ns",
            "range": "± 165.8537798977984"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769755000,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15570.578002929688,
            "unit": "ns",
            "range": "± 7.862999345514603"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15707.70510160006,
            "unit": "ns",
            "range": "± 10.935878093035143"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 16017.008033165564,
            "unit": "ns",
            "range": "± 13.273149852317538"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898802545,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: ACL)",
            "value": 15690.7472882952,
            "unit": "ns",
            "range": "± 24.03484121278283"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: AOF)",
            "value": 15751.497356708232,
            "unit": "ns",
            "range": "± 23.584212717557882"
          },
          {
            "name": "BDN.benchmark.Operations.PubSubOperations.Publish(Params: None)",
            "value": 16639.4037882487,
            "unit": "ns",
            "range": "± 24.119336467057693"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769713593,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 246.3446423457219,
            "unit": "ns",
            "range": "± 0.4150663529832851"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 276.9332227706909,
            "unit": "ns",
            "range": "± 2.569010181406007"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 311.1155297415597,
            "unit": "ns",
            "range": "± 0.30968010901996984"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 316.58450409082263,
            "unit": "ns",
            "range": "± 1.3868671521743396"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 234.41849503914514,
            "unit": "ns",
            "range": "± 0.3414286510277702"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 178.4580156803131,
            "unit": "ns",
            "range": "± 0.1397211814219804"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 300.88281624657765,
            "unit": "ns",
            "range": "± 2.3141585223903394"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 299.0242427587509,
            "unit": "ns",
            "range": "± 0.17164505353248474"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 352.47120389938357,
            "unit": "ns",
            "range": "± 2.4358391277883746"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 366.9256319659097,
            "unit": "ns",
            "range": "± 3.8831382656785522"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898870726,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 242.18944916358362,
            "unit": "ns",
            "range": "± 0.7249633892434937"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 283.17993318117584,
            "unit": "ns",
            "range": "± 1.4759375856253873"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 300.9775372285109,
            "unit": "ns",
            "range": "± 0.6283054674179475"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 316.5830424626668,
            "unit": "ns",
            "range": "± 1.7277729770655565"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 227.35086617469787,
            "unit": "ns",
            "range": "± 0.5618478684389269"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 172.07683867674606,
            "unit": "ns",
            "range": "± 0.4132836064184422"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 302.086387021201,
            "unit": "ns",
            "range": "± 1.406943858405377"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 305.97435679802527,
            "unit": "ns",
            "range": "± 0.3342938025793201"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 352.3552936712901,
            "unit": "ns",
            "range": "± 0.5569328387183564"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 358.2061893258776,
            "unit": "ns",
            "range": "± 1.4820310318570307"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769687131,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 101477.87027994792,
            "unit": "ns",
            "range": "± 335.06348015038407"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 118804.93512834821,
            "unit": "ns",
            "range": "± 495.6978050314257"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 105631.06951032366,
            "unit": "ns",
            "range": "± 297.22414267810785"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898884714,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 102541.15641276042,
            "unit": "ns",
            "range": "± 217.618176655721"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 119311.99300130208,
            "unit": "ns",
            "range": "± 420.4551163569995"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 104953.04652622768,
            "unit": "ns",
            "range": "± 195.58750859410222"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769702302,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 243.6507863998413,
            "unit": "ns",
            "range": "± 0.4955407350550503"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 284.1004352910178,
            "unit": "ns",
            "range": "± 2.767680880155092"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 340.0601002693176,
            "unit": "ns",
            "range": "± 2.2760438227859363"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 326.2888272830418,
            "unit": "ns",
            "range": "± 2.3959107978835292"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 238.36365866661072,
            "unit": "ns",
            "range": "± 0.6466978431943309"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 190.1698757727941,
            "unit": "ns",
            "range": "± 0.6626912586194147"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 333.8875994046529,
            "unit": "ns",
            "range": "± 1.3271834054896474"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 327.83838818868,
            "unit": "ns",
            "range": "± 2.383321224238445"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 379.6924762044634,
            "unit": "ns",
            "range": "± 0.550958095947619"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 372.20372870763146,
            "unit": "ns",
            "range": "± 2.237864452271441"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898867156,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 228.13751086393992,
            "unit": "ns",
            "range": "± 1.6858465602567394"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 288.4448284736046,
            "unit": "ns",
            "range": "± 1.0670328184145754"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 321.29305238723754,
            "unit": "ns",
            "range": "± 2.219785286618458"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 334.8181738512857,
            "unit": "ns",
            "range": "± 1.6701861517907592"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 237.286426226298,
            "unit": "ns",
            "range": "± 0.27805589291613236"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 193.3352986574173,
            "unit": "ns",
            "range": "± 0.2497788646356325"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 310.4737124443054,
            "unit": "ns",
            "range": "± 1.1074528682009466"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 299.36966730998114,
            "unit": "ns",
            "range": "± 1.573435087343809"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 377.56023032324657,
            "unit": "ns",
            "range": "± 1.709076027987185"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 390.32587964194164,
            "unit": "ns",
            "range": "± 0.9594525494493624"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769703259,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16708.557040405274,
            "unit": "ns",
            "range": "± 127.91284126566919"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 16246.39539865347,
            "unit": "ns",
            "range": "± 16.0053392577595"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 15284.412425114559,
            "unit": "ns",
            "range": "± 44.32366478565768"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13986.42132450984,
            "unit": "ns",
            "range": "± 112.78961323315731"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 116902.8684000651,
            "unit": "ns",
            "range": "± 474.60752685975876"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 21525.9034576416,
            "unit": "ns",
            "range": "± 71.93429165399607"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 20331.496551513672,
            "unit": "ns",
            "range": "± 99.84071738289751"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 16452.187789916992,
            "unit": "ns",
            "range": "± 79.91184678972563"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 15267.960043770927,
            "unit": "ns",
            "range": "± 9.178097427637791"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 123279.33665114183,
            "unit": "ns",
            "range": "± 87.38600661493956"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898865351,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 17313.93819972447,
            "unit": "ns",
            "range": "± 66.2020073329056"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 16263.21828351702,
            "unit": "ns",
            "range": "± 93.68328042233041"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 15184.262493426982,
            "unit": "ns",
            "range": "± 10.108844886354582"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 14209.938373819987,
            "unit": "ns",
            "range": "± 63.55390360687628"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 116107.98547363281,
            "unit": "ns",
            "range": "± 465.85833854632193"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 21115.487659999304,
            "unit": "ns",
            "range": "± 83.98068843594294"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 20891.831444295247,
            "unit": "ns",
            "range": "± 95.60954003789847"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15904.47681681315,
            "unit": "ns",
            "range": "± 79.03378510217928"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 15386.289833068848,
            "unit": "ns",
            "range": "± 9.643837662684643"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 123381.42184244792,
            "unit": "ns",
            "range": "± 821.3934784526989"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769749291,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 90855.33203125,
            "unit": "ns",
            "range": "± 149.426447051441"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 104822.3421536959,
            "unit": "ns",
            "range": "± 205.84764520486002"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 92614.36680385044,
            "unit": "ns",
            "range": "± 210.08081026880987"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898842471,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: ACL)",
            "value": 92304.52270507812,
            "unit": "ns",
            "range": "± 170.91025971822893"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: AOF)",
            "value": 106182.99560546875,
            "unit": "ns",
            "range": "± 229.59063077725028"
          },
          {
            "name": "BDN.benchmark.Operations.ObjectOperations.LPushPop(Params: None)",
            "value": 92349.31640625,
            "unit": "ns",
            "range": "± 166.49233620594637"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769712221,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16739.80646623884,
            "unit": "ns",
            "range": "± 239.011377794578"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 15408.392412821451,
            "unit": "ns",
            "range": "± 8.59178611362792"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14659.776344299316,
            "unit": "ns",
            "range": "± 36.19231026788375"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13676.389719645182,
            "unit": "ns",
            "range": "± 85.63985749249147"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 111771.29916178386,
            "unit": "ns",
            "range": "± 536.3137368710018"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19507.857033865792,
            "unit": "ns",
            "range": "± 121.39676297114971"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19187.952180989585,
            "unit": "ns",
            "range": "± 228.14866841476066"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15400.169739786785,
            "unit": "ns",
            "range": "± 67.21389488765945"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 14650.516018676757,
            "unit": "ns",
            "range": "± 72.25197156153506"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 115853.48992047991,
            "unit": "ns",
            "range": "± 526.1898986930147"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898878267,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16638.449935913086,
            "unit": "ns",
            "range": "± 71.4180979042516"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 15222.796097975512,
            "unit": "ns",
            "range": "± 18.92283013421302"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14628.879807608468,
            "unit": "ns",
            "range": "± 20.43613465370193"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13532.564628092448,
            "unit": "ns",
            "range": "± 83.1873938230058"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 113128.43997896634,
            "unit": "ns",
            "range": "± 144.44857725158136"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19556.20578511556,
            "unit": "ns",
            "range": "± 254.16336882608402"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19237.964639516977,
            "unit": "ns",
            "range": "± 41.87665263134305"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15474.532061258951,
            "unit": "ns",
            "range": "± 17.18459547153336"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 14167.75220336914,
            "unit": "ns",
            "range": "± 84.81607760842816"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 117604.71324869791,
            "unit": "ns",
            "range": "± 571.6821842777119"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769768316,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: ACL)",
            "value": 1800.5355834960938,
            "unit": "ns",
            "range": "± 5.651189059138939"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: AOF)",
            "value": 1713.6600934542143,
            "unit": "ns",
            "range": "± 3.8919670972054305"
          },
          {
            "name": "BDN.benchmark.Operations.BasicOperations.InlinePing(Params: None)",
            "value": 1732.6046576866736,
            "unit": "ns",
            "range": "± 1.7546042002509006"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769750769,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 36410.09276529948,
            "unit": "ns",
            "range": "± 285.8552751764316"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 161856.70451472356,
            "unit": "ns",
            "range": "± 588.5518940885396"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 112717.01393345425,
            "unit": "ns",
            "range": "± 702.8858668595357"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 78445.12591552734,
            "unit": "ns",
            "range": "± 53.6552989642733"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 33203.40789358957,
            "unit": "ns",
            "range": "± 290.30018024634546"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 171323.66598307292,
            "unit": "ns",
            "range": "± 1442.6188497212759"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 122716.3314115084,
            "unit": "ns",
            "range": "± 653.4109793850155"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 106825.11704799107,
            "unit": "ns",
            "range": "± 559.2761275413133"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 33731.71775465745,
            "unit": "ns",
            "range": "± 181.29686140913256"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 168434.62877604167,
            "unit": "ns",
            "range": "± 743.904660507621"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 111833.47737339565,
            "unit": "ns",
            "range": "± 485.9837658209682"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 77977.62018291767,
            "unit": "ns",
            "range": "± 74.39539547704118"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769726402,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 31476.480433872766,
            "unit": "ns",
            "range": "± 177.76684578334067"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 139208.2686686198,
            "unit": "ns",
            "range": "± 1365.5662228209226"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 108514.05046198919,
            "unit": "ns",
            "range": "± 560.8548664906808"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 75518.92567795973,
            "unit": "ns",
            "range": "± 87.6000373105464"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 31768.16171468099,
            "unit": "ns",
            "range": "± 271.4886784460146"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 147418.3139485677,
            "unit": "ns",
            "range": "± 968.6484278489708"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 122704.00360514323,
            "unit": "ns",
            "range": "± 1238.8180527424554"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 103426.63819231305,
            "unit": "ns",
            "range": "± 638.7917535625502"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 32218.58612874349,
            "unit": "ns",
            "range": "± 314.1141474059347"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 136852.9156963642,
            "unit": "ns",
            "range": "± 462.0840091464654"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 109835.06352887835,
            "unit": "ns",
            "range": "± 688.1504921488453"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 74080.28588053385,
            "unit": "ns",
            "range": "± 657.2841369402156"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kmontrose@microsoft.com",
            "name": "kevin-montrose",
            "username": "kevin-montrose"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c30f10c87c5331fbbb16596a3061c37904cbcc6d",
          "message": "Add a fuzzing host (#1166)\n\n* sketch out a OneFuzz target, with some local testing niceities\n\n* implement the command parser fuzz target\n\n* parsing exceptions are fine\n\n* derp, messed up this filter\n\n* implement lua compilation fuzzing\n\n* sketch out GarnetEndToEnd fuzz target\n\n* sketch out GarnetEndToEnd fuzz target\n\n* fix bugs; add --input-directory\n\n* bunch of false crashes creating files, rework to be less race-y\n\n* small amount of cleanup and DRY'ing\n\n* formatting\n\n* fix merge\n\n* correctly cleanup LogDir and CheckpointDir in EndToEnd fuzz",
          "timestamp": "2025-04-17T10:02:05-04:00",
          "tree_id": "d5da23e2696ac93e84674aee665baa4413e1f111",
          "url": "https://github.com/microsoft/garnet/commit/c30f10c87c5331fbbb16596a3061c37904cbcc6d"
        },
        "date": 1744898887934,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 30993.53332754282,
            "unit": "ns",
            "range": "± 174.54963124870827"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 139117.56112234932,
            "unit": "ns",
            "range": "± 786.420745390062"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 115059.15545654297,
            "unit": "ns",
            "range": "± 723.551585414665"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 72103.52454552284,
            "unit": "ns",
            "range": "± 448.2146453439487"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 31549.889229329427,
            "unit": "ns",
            "range": "± 329.4001948119764"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 149542.0864746094,
            "unit": "ns",
            "range": "± 1078.0794529482162"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 125542.75451660156,
            "unit": "ns",
            "range": "± 393.1698337430319"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 99043.57608642578,
            "unit": "ns",
            "range": "± 476.96776583967085"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 32288.20719909668,
            "unit": "ns",
            "range": "± 51.916602550974936"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 138782.14229642428,
            "unit": "ns",
            "range": "± 389.66909886547387"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 108291.58440290179,
            "unit": "ns",
            "range": "± 491.4852593345395"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 76214.96872151693,
            "unit": "ns",
            "range": "± 656.1669003439271"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769981028,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 199.40585238592965,
            "unit": "ns",
            "range": "± 0.3571770164608445"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 280.049044745309,
            "unit": "ns",
            "range": "± 0.5004906827135395"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 289.2624048086313,
            "unit": "ns",
            "range": "± 0.4594929892437336"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 302.17780113220215,
            "unit": "ns",
            "range": "± 0.7762304581177099"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 217.02175310679846,
            "unit": "ns",
            "range": "± 0.22705595563626574"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 155.4910843188946,
            "unit": "ns",
            "range": "± 0.23431502372048343"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 292.273629506429,
            "unit": "ns",
            "range": "± 0.5977373570360144"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 300.9930658340454,
            "unit": "ns",
            "range": "± 0.5937709418337038"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 328.807442004864,
            "unit": "ns",
            "range": "± 0.7819959414886544"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 340.71788447243824,
            "unit": "ns",
            "range": "± 0.7071229147382888"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769863772,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 16012.626647949219,
            "unit": "ns",
            "range": "± 10.41854843502833"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 14530.305786132812,
            "unit": "ns",
            "range": "± 11.967717692696764"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14450.762736002604,
            "unit": "ns",
            "range": "± 12.835905002801013"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13278.948211669922,
            "unit": "ns",
            "range": "± 13.78062210181652"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 108536.66730608259,
            "unit": "ns",
            "range": "± 109.29999351704427"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 19543.191324869793,
            "unit": "ns",
            "range": "± 21.705469074800078"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19439.70772879464,
            "unit": "ns",
            "range": "± 21.128597462733108"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15307.410757882255,
            "unit": "ns",
            "range": "± 12.834305992324424"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 13932.768014761117,
            "unit": "ns",
            "range": "± 12.694017421342904"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 115208.26503208706,
            "unit": "ns",
            "range": "± 201.28705817375777"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769968154,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: DSV)",
            "value": 15982.242838541666,
            "unit": "ns",
            "range": "± 19.74213246363706"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: DSV)",
            "value": 14654.532975416918,
            "unit": "ns",
            "range": "± 17.831620501956976"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: DSV)",
            "value": 14266.741333007812,
            "unit": "ns",
            "range": "± 32.08380056604635"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: DSV)",
            "value": 13217.836233285758,
            "unit": "ns",
            "range": "± 16.135921128813585"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: DSV)",
            "value": 106826.83809720553,
            "unit": "ns",
            "range": "± 148.34503292507253"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Get(Params: None)",
            "value": 21395.234797551082,
            "unit": "ns",
            "range": "± 28.888768729256526"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.Set(Params: None)",
            "value": 19854.546915690105,
            "unit": "ns",
            "range": "± 39.716564392490675"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MGet(Params: None)",
            "value": 15385.652974446615,
            "unit": "ns",
            "range": "± 21.129595908196514"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.MSet(Params: None)",
            "value": 13989.409891764322,
            "unit": "ns",
            "range": "± 23.897329613603837"
          },
          {
            "name": "BDN.benchmark.Cluster.ClusterOperations.CTXNSET(Params: None)",
            "value": 120781.8359375,
            "unit": "ns",
            "range": "± 328.8355090772011"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769955509,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 29424.883597237724,
            "unit": "ns",
            "range": "± 23.62914947036985"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 143494.01010366587,
            "unit": "ns",
            "range": "± 168.1302811946992"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 110293.84765625,
            "unit": "ns",
            "range": "± 228.20136553538686"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 83369.13800920759,
            "unit": "ns",
            "range": "± 420.2849541408662"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 30527.39802769252,
            "unit": "ns",
            "range": "± 26.893924694056885"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 144489.94422325722,
            "unit": "ns",
            "range": "± 311.9265154008586"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 112070.1677594866,
            "unit": "ns",
            "range": "± 119.26082215687468"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 104839.00634765625,
            "unit": "ns",
            "range": "± 237.30494216286317"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 30743.857515775242,
            "unit": "ns",
            "range": "± 88.88300344620609"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 142330.30436197916,
            "unit": "ns",
            "range": "± 129.45242452860862"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 106736.32934570312,
            "unit": "ns",
            "range": "± 87.882207291784"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 80947.31022761419,
            "unit": "ns",
            "range": "± 190.98817474957997"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769848093,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Set(Params: None)",
            "value": 210.21491629736764,
            "unit": "ns",
            "range": "± 0.19294350217900746"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetEx(Params: None)",
            "value": 282.85836492265975,
            "unit": "ns",
            "range": "± 0.33639999296928447"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetNx(Params: None)",
            "value": 285.12892723083496,
            "unit": "ns",
            "range": "± 0.7801429890799624"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.SetXx(Params: None)",
            "value": 315.8021484102522,
            "unit": "ns",
            "range": "± 0.3191966597203002"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetFound(Params: None)",
            "value": 217.39191015561423,
            "unit": "ns",
            "range": "± 0.15516509142523496"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.GetNotFound(Params: None)",
            "value": 168.52606534957886,
            "unit": "ns",
            "range": "± 0.22008008014639316"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Increment(Params: None)",
            "value": 301.9603633880615,
            "unit": "ns",
            "range": "± 0.5197580573458824"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.Decrement(Params: None)",
            "value": 296.3320795694987,
            "unit": "ns",
            "range": "± 0.6376135903373895"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.IncrementBy(Params: None)",
            "value": 352.03309976137604,
            "unit": "ns",
            "range": "± 0.49400789946689383"
          },
          {
            "name": "BDN.benchmark.Network.RawStringOperations.DecrementBy(Params: None)",
            "value": 354.1823829923357,
            "unit": "ns",
            "range": "± 0.6349932593410144"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769877679,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 896.8421052631579,
            "unit": "ns",
            "range": "± 669.3045980843079"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 860.6382978723404,
            "unit": "ns",
            "range": "± 593.1904053608124"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 1617.7083333333333,
            "unit": "ns",
            "range": "± 936.6226889220169"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 397932,
            "unit": "ns",
            "range": "± 90977.06879463993"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 1914.4329896907216,
            "unit": "ns",
            "range": "± 1049.8997661207206"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 6007.291666666667,
            "unit": "ns",
            "range": "± 1202.56259639389"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1010.3092783505155,
            "unit": "ns",
            "range": "± 832.7270131024425"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 791.6666666666666,
            "unit": "ns",
            "range": "± 548.6186800869534"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 1661.340206185567,
            "unit": "ns",
            "range": "± 780.0742871488524"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 418000,
            "unit": "ns",
            "range": "± 87642.06591113492"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 2845.8762886597938,
            "unit": "ns",
            "range": "± 1949.835330962442"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 6520.652173913043,
            "unit": "ns",
            "range": "± 1825.071911482705"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1008.2474226804123,
            "unit": "ns",
            "range": "± 835.3729335717851"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 821.0526315789474,
            "unit": "ns",
            "range": "± 589.9683974302399"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 1942.5531914893618,
            "unit": "ns",
            "range": "± 1268.433066179214"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 371336.7816091954,
            "unit": "ns",
            "range": "± 36967.74242794916"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 2253.5353535353534,
            "unit": "ns",
            "range": "± 1509.312124122472"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 6539.130434782609,
            "unit": "ns",
            "range": "± 1469.5410453262987"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1036.0824742268042,
            "unit": "ns",
            "range": "± 876.5683048839646"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 888.8888888888889,
            "unit": "ns",
            "range": "± 539.547135207955"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1656.5217391304348,
            "unit": "ns",
            "range": "± 1162.5383697154596"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 456584,
            "unit": "ns",
            "range": "± 105373.42324928392"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1829.1666666666667,
            "unit": "ns",
            "range": "± 1176.2488697162564"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 6601.030927835051,
            "unit": "ns",
            "range": "± 2054.4917683254025"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 1092.7083333333333,
            "unit": "ns",
            "range": "± 808.7996032674686"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 715.7894736842105,
            "unit": "ns",
            "range": "± 566.6842322113386"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 1784.5360824742268,
            "unit": "ns",
            "range": "± 1067.741015550428"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 436025.2873563218,
            "unit": "ns",
            "range": "± 66851.23230046613"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 1893.75,
            "unit": "ns",
            "range": "± 1055.6925888274784"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 8746.808510638299,
            "unit": "ns",
            "range": "± 2031.2452945935684"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769855685,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 6131,
            "unit": "ns",
            "range": "± 2770.614015015809"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 6818.367346938776,
            "unit": "ns",
            "range": "± 2318.4311678719077"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 340418.75,
            "unit": "ns",
            "range": "± 67042.44737477892"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 363726.2626262626,
            "unit": "ns",
            "range": "± 73474.81532409534"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 28217.171717171717,
            "unit": "ns",
            "range": "± 9370.144080133303"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 132369.38775510204,
            "unit": "ns",
            "range": "± 22095.236264875584"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 7530.6122448979595,
            "unit": "ns",
            "range": "± 3773.166467080683"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 5838.775510204082,
            "unit": "ns",
            "range": "± 2300.454130561344"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 357523.23232323234,
            "unit": "ns",
            "range": "± 77565.34155055103"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 345864,
            "unit": "ns",
            "range": "± 76164.15222956588"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 26118.0412371134,
            "unit": "ns",
            "range": "± 6041.239290489036"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 132094.79166666666,
            "unit": "ns",
            "range": "± 22034.10929871501"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 6275.757575757576,
            "unit": "ns",
            "range": "± 2558.821911209113"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 8132.65306122449,
            "unit": "ns",
            "range": "± 3871.1136283701894"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 378904,
            "unit": "ns",
            "range": "± 74857.87330641711"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 380119,
            "unit": "ns",
            "range": "± 64634.991766117375"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 36137.878787878784,
            "unit": "ns",
            "range": "± 7264.412454678512"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 139248.95833333334,
            "unit": "ns",
            "range": "± 21014.739162345166"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 4974.489795918367,
            "unit": "ns",
            "range": "± 2665.4359317155554"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 8934.69387755102,
            "unit": "ns",
            "range": "± 2734.2464251728256"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 453716,
            "unit": "ns",
            "range": "± 88511.79310799166"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 480705,
            "unit": "ns",
            "range": "± 115308.89071436677"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 34939.795918367345,
            "unit": "ns",
            "range": "± 7595.749486864326"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 135409.27835051547,
            "unit": "ns",
            "range": "± 23445.362381122002"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 6530.208333333333,
            "unit": "ns",
            "range": "± 2907.0556561096837"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 5156.122448979592,
            "unit": "ns",
            "range": "± 1923.9170913899577"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 470297,
            "unit": "ns",
            "range": "± 101261.26669325854"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 440240,
            "unit": "ns",
            "range": "± 93516.29307616438"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 35728.865979381444,
            "unit": "ns",
            "range": "± 9014.83683022612"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 142253.53535353535,
            "unit": "ns",
            "range": "± 21869.041173849702"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769932972,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 270.8291177431742,
            "unit": "ns",
            "range": "± 2.1425900540826475"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 366.8685404913766,
            "unit": "ns",
            "range": "± 1.5997458390720771"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 643.275095240275,
            "unit": "ns",
            "range": "± 3.0984431359257814"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 925.6890548706054,
            "unit": "ns",
            "range": "± 3.0069331003213544"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 274.7478965123494,
            "unit": "ns",
            "range": "± 1.939592283917676"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 358.5472312654768,
            "unit": "ns",
            "range": "± 2.0591216964669106"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 625.8409976959229,
            "unit": "ns",
            "range": "± 1.3336212108651797"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 893.1175654093424,
            "unit": "ns",
            "range": "± 2.7216931287253856"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 277.30081437428794,
            "unit": "ns",
            "range": "± 2.27749823283778"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 362.8069652829851,
            "unit": "ns",
            "range": "± 1.558961373988274"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 634.8538078161387,
            "unit": "ns",
            "range": "± 1.8688634839035487"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 869.5374580792019,
            "unit": "ns",
            "range": "± 2.025448515686205"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 275.7022552887599,
            "unit": "ns",
            "range": "± 0.32590712377167563"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 353.57359087467194,
            "unit": "ns",
            "range": "± 0.46496067594363866"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 622.0584994633992,
            "unit": "ns",
            "range": "± 2.8393807713417196"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 889.0017240388053,
            "unit": "ns",
            "range": "± 2.0726638050802273"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 272.1570406326881,
            "unit": "ns",
            "range": "± 0.27522582272803686"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 354.1153913644644,
            "unit": "ns",
            "range": "± 0.7424112489633095"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 650.2117884499686,
            "unit": "ns",
            "range": "± 1.8391511158392106"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 859.400402768453,
            "unit": "ns",
            "range": "± 2.3554086254144893"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769908020,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,Limit)",
            "value": 5724.489795918367,
            "unit": "ns",
            "range": "± 1944.437544863343"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,Limit)",
            "value": 4763.265306122449,
            "unit": "ns",
            "range": "± 2053.178966147703"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,Limit)",
            "value": 336448,
            "unit": "ns",
            "range": "± 74116.88759157812"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,Limit)",
            "value": 320239,
            "unit": "ns",
            "range": "± 65883.17621427194"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,Limit)",
            "value": 20763.917525773195,
            "unit": "ns",
            "range": "± 7213.719359188236"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,Limit)",
            "value": 126142.42424242424,
            "unit": "ns",
            "range": "± 27954.792553623804"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Managed,None)",
            "value": 4798.989898989899,
            "unit": "ns",
            "range": "± 1903.9430057868544"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Managed,None)",
            "value": 6270.408163265306,
            "unit": "ns",
            "range": "± 2869.9010070816266"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Managed,None)",
            "value": 322414,
            "unit": "ns",
            "range": "± 68023.83275163361"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Managed,None)",
            "value": 329718,
            "unit": "ns",
            "range": "± 69435.19379517664"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Managed,None)",
            "value": 23597.916666666668,
            "unit": "ns",
            "range": "± 7619.669548942903"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Managed,None)",
            "value": 122344.44444444444,
            "unit": "ns",
            "range": "± 25866.834104234007"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Native,None)",
            "value": 5011.578947368421,
            "unit": "ns",
            "range": "± 1744.710130035917"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Native,None)",
            "value": 5219.072164948454,
            "unit": "ns",
            "range": "± 1955.513616714197"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Native,None)",
            "value": 352095,
            "unit": "ns",
            "range": "± 81575.05359291447"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Native,None)",
            "value": 362417,
            "unit": "ns",
            "range": "± 72139.85302833306"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Native,None)",
            "value": 24787.23404255319,
            "unit": "ns",
            "range": "± 5803.192188704739"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Native,None)",
            "value": 131545.45454545456,
            "unit": "ns",
            "range": "± 31262.467791160074"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,Limit)",
            "value": 5663.131313131313,
            "unit": "ns",
            "range": "± 1802.98202333203"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,Limit)",
            "value": 5340.625,
            "unit": "ns",
            "range": "± 2005.2341049836236"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,Limit)",
            "value": 438625.7894736842,
            "unit": "ns",
            "range": "± 80919.27776492917"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,Limit)",
            "value": 471963,
            "unit": "ns",
            "range": "± 103129.48579040788"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,Limit)",
            "value": 30866.842105263157,
            "unit": "ns",
            "range": "± 8351.833186863048"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,Limit)",
            "value": 140308.33333333334,
            "unit": "ns",
            "range": "± 31339.266328921723"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersSmall(Params: Tracked,None)",
            "value": 6031.632653061224,
            "unit": "ns",
            "range": "± 2068.247773685681"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ResetParametersLarge(Params: Tracked,None)",
            "value": 5719.3877551020405,
            "unit": "ns",
            "range": "± 2086.4144829078646"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructSmall(Params: Tracked,None)",
            "value": 458863,
            "unit": "ns",
            "range": "± 105550.8634677513"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.ConstructLarge(Params: Tracked,None)",
            "value": 426996.4705882353,
            "unit": "ns",
            "range": "± 58412.21016978185"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionSmall(Params: Tracked,None)",
            "value": 28152.04081632653,
            "unit": "ns",
            "range": "± 7490.202942646361"
          },
          {
            "name": "BDN.benchmark.Lua.LuaRunnerOperations.CompileForSessionLarge(Params: Tracked,None)",
            "value": 139257.8947368421,
            "unit": "ns",
            "range": "± 34811.96168519925"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769911845,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,Limit)",
            "value": 1276.7045454545455,
            "unit": "ns",
            "range": "± 909.7160370275323"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,Limit)",
            "value": 792.3913043478261,
            "unit": "ns",
            "range": "± 576.1245469079829"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,Limit)",
            "value": 2528.8888888888887,
            "unit": "ns",
            "range": "± 1173.9919840130262"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,Limit)",
            "value": 372169.4117647059,
            "unit": "ns",
            "range": "± 44564.188653717756"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,Limit)",
            "value": 2517.7083333333335,
            "unit": "ns",
            "range": "± 1272.9571697980846"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,Limit)",
            "value": 8058.163265306122,
            "unit": "ns",
            "range": "± 2860.4636445390165"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Managed,None)",
            "value": 1536.5591397849462,
            "unit": "ns",
            "range": "± 910.3015441905903"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Managed,None)",
            "value": 863.4408602150538,
            "unit": "ns",
            "range": "± 585.6592246714209"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Managed,None)",
            "value": 2279.775280898876,
            "unit": "ns",
            "range": "± 820.0858845379464"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Managed,None)",
            "value": 424116,
            "unit": "ns",
            "range": "± 78499.7699539832"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Managed,None)",
            "value": 3665.7894736842104,
            "unit": "ns",
            "range": "± 2101.5351931321843"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Managed,None)",
            "value": 7902.577319587629,
            "unit": "ns",
            "range": "± 3041.7269212127244"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Native,None)",
            "value": 1275.8241758241759,
            "unit": "ns",
            "range": "± 1058.0212830605228"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Native,None)",
            "value": 925.8426966292135,
            "unit": "ns",
            "range": "± 614.6928445624548"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Native,None)",
            "value": 2117.391304347826,
            "unit": "ns",
            "range": "± 1223.8121575007463"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Native,None)",
            "value": 430351.02040816325,
            "unit": "ns",
            "range": "± 85695.93964192811"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Native,None)",
            "value": 2051.190476190476,
            "unit": "ns",
            "range": "± 851.1500437740914"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Native,None)",
            "value": 7260.606060606061,
            "unit": "ns",
            "range": "± 2819.0249501838066"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,Limit)",
            "value": 1563.7362637362637,
            "unit": "ns",
            "range": "± 1179.5023888173714"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,Limit)",
            "value": 756.3829787234042,
            "unit": "ns",
            "range": "± 611.9875096480532"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,Limit)",
            "value": 1923.404255319149,
            "unit": "ns",
            "range": "± 1401.6254413469953"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,Limit)",
            "value": 479822.44897959183,
            "unit": "ns",
            "range": "± 100787.47240575423"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,Limit)",
            "value": 1912.3595505617977,
            "unit": "ns",
            "range": "± 854.7753857964279"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,Limit)",
            "value": 7341.935483870968,
            "unit": "ns",
            "range": "± 2542.94944215524"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupHit(Params: Tracked,None)",
            "value": 961.5384615384615,
            "unit": "ns",
            "range": "± 950.0607268040209"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LookupMiss(Params: Tracked,None)",
            "value": 854.2553191489362,
            "unit": "ns",
            "range": "± 752.7247842192089"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadOuterHit(Params: Tracked,None)",
            "value": 2156.818181818182,
            "unit": "ns",
            "range": "± 1183.8759113432138"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadInnerHit(Params: Tracked,None)",
            "value": 487969,
            "unit": "ns",
            "range": "± 104385.0725569862"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.LoadMiss(Params: Tracked,None)",
            "value": 2243.010752688172,
            "unit": "ns",
            "range": "± 1411.2435041990423"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScriptCacheOperations.Digest(Params: Tracked,None)",
            "value": 8371.134020618556,
            "unit": "ns",
            "range": "± 2899.495601353456"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769912865,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: ACL)",
            "value": 32334.20633951823,
            "unit": "ns",
            "range": "± 62.8888272460361"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: ACL)",
            "value": 154454.24630301338,
            "unit": "ns",
            "range": "± 251.0740088739791"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: ACL)",
            "value": 105171.48672250602,
            "unit": "ns",
            "range": "± 76.77042558762365"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: ACL)",
            "value": 82000.31080979567,
            "unit": "ns",
            "range": "± 78.3412796860548"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: AOF)",
            "value": 30863.97892878606,
            "unit": "ns",
            "range": "± 53.42997296857411"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: AOF)",
            "value": 166951.4168294271,
            "unit": "ns",
            "range": "± 709.0167215658118"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: AOF)",
            "value": 118435.82600911458,
            "unit": "ns",
            "range": "± 149.80858178470015"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: AOF)",
            "value": 109996.25162760417,
            "unit": "ns",
            "range": "± 185.08323660457845"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomRawStringCommand(Params: None)",
            "value": 30132.979910714286,
            "unit": "ns",
            "range": "± 38.31846395982961"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomObjectCommand(Params: None)",
            "value": 149445.4380580357,
            "unit": "ns",
            "range": "± 252.38990835885417"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomTransaction(Params: None)",
            "value": 104387.36572265625,
            "unit": "ns",
            "range": "± 151.49091166969237"
          },
          {
            "name": "BDN.benchmark.Operations.CustomOperations.CustomProcedure(Params: None)",
            "value": 84863.81648137019,
            "unit": "ns",
            "range": "± 82.68346207651194"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744769925101,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 289.418530532292,
            "unit": "ns",
            "range": "± 1.2253512039916283"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 363.67189168930054,
            "unit": "ns",
            "range": "± 0.8633809953967195"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 625.1237843831381,
            "unit": "ns",
            "range": "± 2.6641995378225207"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 881.1423717226301,
            "unit": "ns",
            "range": "± 2.7867209236218953"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 286.2662899017334,
            "unit": "ns",
            "range": "± 2.100240951226797"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 374.2962595735277,
            "unit": "ns",
            "range": "± 0.9930548399332242"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 699.5877006530761,
            "unit": "ns",
            "range": "± 2.622996668344906"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 861.7754205703735,
            "unit": "ns",
            "range": "± 2.905893964706886"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 287.7217712402344,
            "unit": "ns",
            "range": "± 1.7584715088229004"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 360.9297189031328,
            "unit": "ns",
            "range": "± 0.9678616244321011"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 656.142149411715,
            "unit": "ns",
            "range": "± 1.3303179881895084"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 866.6474438349406,
            "unit": "ns",
            "range": "± 3.2254134521483584"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 291.1104187965393,
            "unit": "ns",
            "range": "± 0.6260290507244706"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 354.35627273412854,
            "unit": "ns",
            "range": "± 0.6583974575741368"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 628.9485531534467,
            "unit": "ns",
            "range": "± 1.9985925393663733"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 878.7487295014517,
            "unit": "ns",
            "range": "± 3.5469811542426406"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 286.4490987141927,
            "unit": "ns",
            "range": "± 1.6455841174757473"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 353.8341158866882,
            "unit": "ns",
            "range": "± 1.4777377320907892"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 622.2898792120127,
            "unit": "ns",
            "range": "± 1.6979393476535816"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 856.3767660004752,
            "unit": "ns",
            "range": "± 2.9420879705844483"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770014888,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 29622.884229532876,
            "unit": "ns",
            "range": "± 104.51067061767037"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 38416.62289632161,
            "unit": "ns",
            "range": "± 218.92805221436763"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 76266.16524251302,
            "unit": "ns",
            "range": "± 315.10702349400606"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 55103.01193237305,
            "unit": "ns",
            "range": "± 234.7097719761002"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 16030.389945983887,
            "unit": "ns",
            "range": "± 9.738133650559037"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 29379.297451782226,
            "unit": "ns",
            "range": "± 147.81418384288017"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 159242.89178059896,
            "unit": "ns",
            "range": "± 1021.3505426237961"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 307021.65533854166,
            "unit": "ns",
            "range": "± 2710.9347824539745"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 30411.779852730888,
            "unit": "ns",
            "range": "± 95.58025267004963"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 47355.48679199219,
            "unit": "ns",
            "range": "± 339.98881589982153"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 85739.52839878628,
            "unit": "ns",
            "range": "± 330.9134340344511"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 52285.611661783856,
            "unit": "ns",
            "range": "± 206.60918278553106"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15788.326713053386,
            "unit": "ns",
            "range": "± 82.15966128155591"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 36089.180891927084,
            "unit": "ns",
            "range": "± 296.18734273486336"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 157951.53684779577,
            "unit": "ns",
            "range": "± 606.2218381069559"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 321870.92301432294,
            "unit": "ns",
            "range": "± 2448.5145438611416"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 29974.571337890626,
            "unit": "ns",
            "range": "± 120.15377814998926"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 38910.96096293131,
            "unit": "ns",
            "range": "± 70.96941022459433"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 74594.07216796876,
            "unit": "ns",
            "range": "± 331.4397050715295"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 58271.20869547526,
            "unit": "ns",
            "range": "± 212.39903096814436"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 16082.168589274088,
            "unit": "ns",
            "range": "± 17.372619147239078"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 28337.62481689453,
            "unit": "ns",
            "range": "± 109.92879460537215"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 164933.90447126116,
            "unit": "ns",
            "range": "± 640.8535682385061"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 303668.4173502604,
            "unit": "ns",
            "range": "± 2972.457045759268"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770001833,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 186.96993192036948,
            "unit": "ns",
            "range": "± 0.883372838774975"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 219.62755719820657,
            "unit": "ns",
            "range": "± 0.16159244689007668"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 306.4063358306885,
            "unit": "ns",
            "range": "± 0.8036972554993226"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 353.3567174275716,
            "unit": "ns",
            "range": "± 0.7154108635089566"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 161.81561580071082,
            "unit": "ns",
            "range": "± 0.3572266996052719"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 192.89281538554602,
            "unit": "ns",
            "range": "± 0.27024278499892035"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 332.32507024492537,
            "unit": "ns",
            "range": "± 0.41845382972822875"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 348.8942657198225,
            "unit": "ns",
            "range": "± 0.7240383858677255"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 161.27267054149084,
            "unit": "ns",
            "range": "± 0.18076553921239505"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 191.9306182861328,
            "unit": "ns",
            "range": "± 0.34898157648986633"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 318.2470194498698,
            "unit": "ns",
            "range": "± 1.0270309543987794"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 357.9159577687581,
            "unit": "ns",
            "range": "± 0.3582637196293877"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 154.76049643296463,
            "unit": "ns",
            "range": "± 0.259762514228123"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 209.07177414212907,
            "unit": "ns",
            "range": "± 0.6460542374200724"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 312.78814633687335,
            "unit": "ns",
            "range": "± 0.4259817032745177"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 365.8747871716817,
            "unit": "ns",
            "range": "± 0.6539984913197145"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 155.78433195749918,
            "unit": "ns",
            "range": "± 0.41030321139772213"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 188.58553171157837,
            "unit": "ns",
            "range": "± 0.423597103774463"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 310.9124739964803,
            "unit": "ns",
            "range": "± 0.5151380916618313"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 355.7687282562256,
            "unit": "ns",
            "range": "± 0.68117524606061"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770027385,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 28642.98031616211,
            "unit": "ns",
            "range": "± 28.23093734624264"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 37948.55021158854,
            "unit": "ns",
            "range": "± 279.42703327810324"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 54129.807373046875,
            "unit": "ns",
            "range": "± 221.22923590620968"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 58648.547110421314,
            "unit": "ns",
            "range": "± 164.04801214122134"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 15162.124232991537,
            "unit": "ns",
            "range": "± 84.56811915286532"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 29124.884767659507,
            "unit": "ns",
            "range": "± 146.6623474917442"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 140417.88665364584,
            "unit": "ns",
            "range": "± 1055.2619359438672"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 238269.39327298678,
            "unit": "ns",
            "range": "± 1683.2131950304647"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 28821.13919576009,
            "unit": "ns",
            "range": "± 183.050101629299"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 43834.58798828125,
            "unit": "ns",
            "range": "± 170.61808542503476"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 62254.6344156901,
            "unit": "ns",
            "range": "± 585.0753150705845"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 56768.46795654297,
            "unit": "ns",
            "range": "± 172.29989725499135"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 16092.325948079428,
            "unit": "ns",
            "range": "± 9.700750149917367"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 34048.03265991211,
            "unit": "ns",
            "range": "± 161.52985170865165"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 139910.64780970983,
            "unit": "ns",
            "range": "± 658.496031982534"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 252993.47872721354,
            "unit": "ns",
            "range": "± 1925.5956711743543"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 28443.7779683431,
            "unit": "ns",
            "range": "± 146.9373575874789"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 38454.89828725962,
            "unit": "ns",
            "range": "± 153.36772777238082"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 53854.447588239396,
            "unit": "ns",
            "range": "± 256.072742767736"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 58126.92362467448,
            "unit": "ns",
            "range": "± 223.2253340649102"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 15047.923423258464,
            "unit": "ns",
            "range": "± 59.086720610907776"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 27487.528241475422,
            "unit": "ns",
            "range": "± 44.21164411709737"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 148836.75061848958,
            "unit": "ns",
            "range": "± 1757.595074705114"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 242587.90270182292,
            "unit": "ns",
            "range": "± 1503.6292708161989"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770013993,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,Limit)",
            "value": 163.98569620572604,
            "unit": "ns",
            "range": "± 0.13923932919864654"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,Limit)",
            "value": 190.73046525319418,
            "unit": "ns",
            "range": "± 0.499126149169512"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,Limit)",
            "value": 322.6707861973689,
            "unit": "ns",
            "range": "± 0.34447730763288303"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,Limit)",
            "value": 363.9162826538086,
            "unit": "ns",
            "range": "± 0.4239500051401495"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Managed,None)",
            "value": 158.09564957251916,
            "unit": "ns",
            "range": "± 0.3069019015547702"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Managed,None)",
            "value": 199.5094861303057,
            "unit": "ns",
            "range": "± 0.32908016036707227"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Managed,None)",
            "value": 331.8468157450358,
            "unit": "ns",
            "range": "± 0.6796364054071523"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Managed,None)",
            "value": 355.50800470205456,
            "unit": "ns",
            "range": "± 1.0915872905426751"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Native,None)",
            "value": 158.42845269611902,
            "unit": "ns",
            "range": "± 0.23034752911504636"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Native,None)",
            "value": 191.28023465474448,
            "unit": "ns",
            "range": "± 0.5987856745360025"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Native,None)",
            "value": 330.3908284505208,
            "unit": "ns",
            "range": "± 0.9500958050468392"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Native,None)",
            "value": 360.2239938882681,
            "unit": "ns",
            "range": "± 0.492090319979196"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,Limit)",
            "value": 157.93029149373373,
            "unit": "ns",
            "range": "± 0.19450690575616586"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,Limit)",
            "value": 200.98880254305325,
            "unit": "ns",
            "range": "± 0.36595795070416726"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,Limit)",
            "value": 324.62316513061523,
            "unit": "ns",
            "range": "± 0.5902386464286222"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,Limit)",
            "value": 372.84631729125977,
            "unit": "ns",
            "range": "± 0.8696716445214621"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script1(Params: Tracked,None)",
            "value": 156.40682990734393,
            "unit": "ns",
            "range": "± 0.3989301208993792"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script2(Params: Tracked,None)",
            "value": 190.34470717112222,
            "unit": "ns",
            "range": "± 0.1674866501323224"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script3(Params: Tracked,None)",
            "value": 307.37990140914917,
            "unit": "ns",
            "range": "± 0.3863528351622414"
          },
          {
            "name": "BDN.benchmark.Lua.LuaScripts.Script4(Params: Tracked,None)",
            "value": 363.4206708272298,
            "unit": "ns",
            "range": "± 1.1709792142380502"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770113627,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 16126.381624494281,
            "unit": "ns",
            "range": "± 95.74053455216178"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 20272.32401123047,
            "unit": "ns",
            "range": "± 223.91977495492017"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 21384.46098836263,
            "unit": "ns",
            "range": "± 22.668530650064923"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 23228.507727050783,
            "unit": "ns",
            "range": "± 157.52906983301335"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15888.374829101562,
            "unit": "ns",
            "range": "± 76.2494345339486"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 10924.9392384847,
            "unit": "ns",
            "range": "± 53.969618850265086"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 22406.36082356771,
            "unit": "ns",
            "range": "± 131.68479876556597"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 21764.490692138672,
            "unit": "ns",
            "range": "± 132.04315975141486"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 28142.77052961077,
            "unit": "ns",
            "range": "± 103.18526945844432"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 26621.870794677736,
            "unit": "ns",
            "range": "± 163.30746270671133"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 21680.222778320312,
            "unit": "ns",
            "range": "± 97.39336769123732"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26103.77811686198,
            "unit": "ns",
            "range": "± 135.7323660800016"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 27722.311815389,
            "unit": "ns",
            "range": "± 123.73917564063983"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 29228.63752746582,
            "unit": "ns",
            "range": "± 167.58534577848687"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 16111.40763268104,
            "unit": "ns",
            "range": "± 46.953932835202274"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10650.841505784254,
            "unit": "ns",
            "range": "± 17.882212198097854"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 29492.513433837892,
            "unit": "ns",
            "range": "± 116.07994848145233"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 30365.441964285714,
            "unit": "ns",
            "range": "± 144.5079121262752"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 36593.0418741862,
            "unit": "ns",
            "range": "± 149.44737786243473"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 34563.79262898763,
            "unit": "ns",
            "range": "± 255.75803413811414"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14627.401663643974,
            "unit": "ns",
            "range": "± 57.37876894079686"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19733.70261891683,
            "unit": "ns",
            "range": "± 17.609923831965737"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 20977.21914085975,
            "unit": "ns",
            "range": "± 10.8201044004201"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 22440.66877644857,
            "unit": "ns",
            "range": "± 94.89134608351392"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 16792.156762259347,
            "unit": "ns",
            "range": "± 64.34589657652074"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10394.98121207101,
            "unit": "ns",
            "range": "± 36.05439292080982"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 22417.8413289388,
            "unit": "ns",
            "range": "± 14.109019679452997"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 21366.108921595984,
            "unit": "ns",
            "range": "± 83.2979126861127"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 27147.698423258462,
            "unit": "ns",
            "range": "± 90.7844331776351"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 27627.269421895344,
            "unit": "ns",
            "range": "± 19.331728651572288"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770250836,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 31335.161743164062,
            "unit": "ns",
            "range": "± 42.70991910035155"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 46745.62032063802,
            "unit": "ns",
            "range": "± 59.83142171532356"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 64172.555338541664,
            "unit": "ns",
            "range": "± 89.13754340336584"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 49000.3515625,
            "unit": "ns",
            "range": "± 49.50594513938389"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 16965.155639648438,
            "unit": "ns",
            "range": "± 60.33149081243342"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 26154.583304268974,
            "unit": "ns",
            "range": "± 85.26132731698986"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 145736.67515345983,
            "unit": "ns",
            "range": "± 470.4124080792548"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 227503.7109375,
            "unit": "ns",
            "range": "± 2700.4745516878706"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 31266.569301060266,
            "unit": "ns",
            "range": "± 45.322153607358594"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 54179.661865234375,
            "unit": "ns",
            "range": "± 117.76110095346732"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 71860.00540597098,
            "unit": "ns",
            "range": "± 283.57062950260956"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 49571.85645470252,
            "unit": "ns",
            "range": "± 51.97374552599768"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 15408.454284667969,
            "unit": "ns",
            "range": "± 29.487541720625835"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 30531.435953776043,
            "unit": "ns",
            "range": "± 97.79530395885006"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 131096.88023158483,
            "unit": "ns",
            "range": "± 354.5930912492851"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 254977.6123046875,
            "unit": "ns",
            "range": "± 1035.0561377583476"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 31435.479501577523,
            "unit": "ns",
            "range": "± 39.33401924320231"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 49724.52110877404,
            "unit": "ns",
            "range": "± 51.92669286995113"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 66165.11753627232,
            "unit": "ns",
            "range": "± 66.06281183950226"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 48527.29750413161,
            "unit": "ns",
            "range": "± 48.56610015069257"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 17738.86893136161,
            "unit": "ns",
            "range": "± 37.5838027542068"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 26260.008341471355,
            "unit": "ns",
            "range": "± 107.95197856883729"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 136604.08732096353,
            "unit": "ns",
            "range": "± 329.25700267814943"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 218922.5838216146,
            "unit": "ns",
            "range": "± 1388.3022975565602"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770128443,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 14792.093271891275,
            "unit": "ns",
            "range": "± 66.49057806288582"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 18418.08386884417,
            "unit": "ns",
            "range": "± 115.89606400237167"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 21228.853431114785,
            "unit": "ns",
            "range": "± 93.75949948085555"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 21149.400012207032,
            "unit": "ns",
            "range": "± 132.39074357570223"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15781.231361897786,
            "unit": "ns",
            "range": "± 131.62713729531362"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 9936.253426688058,
            "unit": "ns",
            "range": "± 67.96118968363845"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 23810.792988368445,
            "unit": "ns",
            "range": "± 95.1365676383273"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 22261.62934366862,
            "unit": "ns",
            "range": "± 154.17815547242424"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 27405.310841151648,
            "unit": "ns",
            "range": "± 247.8779596183728"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 28209.2295308431,
            "unit": "ns",
            "range": "± 182.34983728085203"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 20562.60899251302,
            "unit": "ns",
            "range": "± 155.5991575991983"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 25915.568320138114,
            "unit": "ns",
            "range": "± 153.53481242055236"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 29329.30154636928,
            "unit": "ns",
            "range": "± 122.88531188807158"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 29842.143958536784,
            "unit": "ns",
            "range": "± 134.7778082386318"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15719.324221097506,
            "unit": "ns",
            "range": "± 12.538817011946156"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 9891.152560424805,
            "unit": "ns",
            "range": "± 62.01962503933372"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 27690.976458740235,
            "unit": "ns",
            "range": "± 122.52540517350941"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 27369.267045084634,
            "unit": "ns",
            "range": "± 146.34249715713875"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 31895.95201619466,
            "unit": "ns",
            "range": "± 81.96710188962189"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 32610.96321927584,
            "unit": "ns",
            "range": "± 97.44036653326008"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14328.702424269457,
            "unit": "ns",
            "range": "± 17.33678219375124"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19243.56973520915,
            "unit": "ns",
            "range": "± 24.022191755152082"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 19880.6726369222,
            "unit": "ns",
            "range": "± 125.78166399411356"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 22554.72167867025,
            "unit": "ns",
            "range": "± 129.89993405849197"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15521.629791259766,
            "unit": "ns",
            "range": "± 22.326149943904323"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10084.927171325684,
            "unit": "ns",
            "range": "± 67.6994099574328"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 22632.97145690918,
            "unit": "ns",
            "range": "± 114.53079093643356"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 21897.63840375628,
            "unit": "ns",
            "range": "± 110.76461355138665"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 27093.175835164388,
            "unit": "ns",
            "range": "± 150.0815555352711"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 27343.47825404576,
            "unit": "ns",
            "range": "± 93.84567461247133"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770163064,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: ACL)",
            "value": 32208.673095703125,
            "unit": "ns",
            "range": "± 38.08794132957929"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: ACL)",
            "value": 49304.700646033656,
            "unit": "ns",
            "range": "± 90.23468600484287"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: ACL)",
            "value": 69804.14603097098,
            "unit": "ns",
            "range": "± 79.77227188508267"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: ACL)",
            "value": 54226.01083608774,
            "unit": "ns",
            "range": "± 67.29255037408076"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: ACL)",
            "value": 17429.54646519252,
            "unit": "ns",
            "range": "± 32.12022375139384"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: ACL)",
            "value": 28859.342134915867,
            "unit": "ns",
            "range": "± 27.90086819782902"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: ACL)",
            "value": 165912.50871930804,
            "unit": "ns",
            "range": "± 325.14956260845133"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: ACL)",
            "value": 281462.8731863839,
            "unit": "ns",
            "range": "± 738.7402443716802"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: AOF)",
            "value": 32096.681111653645,
            "unit": "ns",
            "range": "± 52.1358952237862"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: AOF)",
            "value": 56737.80997140067,
            "unit": "ns",
            "range": "± 117.80035311201696"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: AOF)",
            "value": 79219.89868164062,
            "unit": "ns",
            "range": "± 185.50712720729877"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: AOF)",
            "value": 53287.343052455355,
            "unit": "ns",
            "range": "± 63.07566351644408"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: AOF)",
            "value": 17773.894391741072,
            "unit": "ns",
            "range": "± 29.12590024333096"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: AOF)",
            "value": 33113.32560221354,
            "unit": "ns",
            "range": "± 115.625946431969"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: AOF)",
            "value": 161896.53695913462,
            "unit": "ns",
            "range": "± 316.6601505852704"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: AOF)",
            "value": 303857.5753348214,
            "unit": "ns",
            "range": "± 1672.2962446050983"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringReadCommand(Params: None)",
            "value": 33317.74640764509,
            "unit": "ns",
            "range": "± 39.16158956822448"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpRawStringRmwCommand(Params: None)",
            "value": 49907.3838297526,
            "unit": "ns",
            "range": "± 63.8488028321567"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjRmwCommand(Params: None)",
            "value": 74173.27880859375,
            "unit": "ns",
            "range": "± 167.17475632407235"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpObjReadCommand(Params: None)",
            "value": 53138.67928641183,
            "unit": "ns",
            "range": "± 81.0592689378488"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpProc(Params: None)",
            "value": 17617.525155203683,
            "unit": "ns",
            "range": "± 26.654957146040246"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleNoOpTxn(Params: None)",
            "value": 29719.088338216145,
            "unit": "ns",
            "range": "± 47.59970196276625"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonGetCommand(Params: None)",
            "value": 156962.95735677084,
            "unit": "ns",
            "range": "± 558.2976211566067"
          },
          {
            "name": "BDN.benchmark.Operations.ModuleOperations.ModuleJsonSetCommand(Params: None)",
            "value": 271586.5304129464,
            "unit": "ns",
            "range": "± 818.1999521840612"
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
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770255732,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 139380.81952776227,
            "unit": "ns",
            "range": "± 739.3783946701528"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 18748.288665771484,
            "unit": "ns",
            "range": "± 21.1246783247497"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 17377.871892293293,
            "unit": "ns",
            "range": "± 19.204634602110133"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 143937.73523888222,
            "unit": "ns",
            "range": "± 199.96588326097486"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 46760.934077962236,
            "unit": "ns",
            "range": "± 132.9436568153193"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 130561.79298618862,
            "unit": "ns",
            "range": "± 519.0972991753755"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 10550695.296875,
            "unit": "ns",
            "range": "± 141883.05742336405"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 283088.60778808594,
            "unit": "ns",
            "range": "± 13915.588619144723"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 141423.90938895088,
            "unit": "ns",
            "range": "± 546.1657304592924"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 18850.726188072793,
            "unit": "ns",
            "range": "± 31.01399218821931"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 16787.0060740153,
            "unit": "ns",
            "range": "± 78.4421441867219"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 144799.11721191407,
            "unit": "ns",
            "range": "± 977.6294887298071"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 45690.9763671875,
            "unit": "ns",
            "range": "± 175.57361212904385"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 133270.2634765625,
            "unit": "ns",
            "range": "± 524.9570008781699"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 10378932.65,
            "unit": "ns",
            "range": "± 174417.57453756043"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 283530.1713657924,
            "unit": "ns",
            "range": "± 13695.769051461712"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 140282.6118815104,
            "unit": "ns",
            "range": "± 671.404659670861"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 19090.652614339193,
            "unit": "ns",
            "range": "± 80.38196941652818"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 16787.3440633138,
            "unit": "ns",
            "range": "± 95.94951787760405"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 142656.49432842547,
            "unit": "ns",
            "range": "± 426.39694473249733"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 47939.27944539388,
            "unit": "ns",
            "range": "± 117.76542320711759"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 126661.06793619791,
            "unit": "ns",
            "range": "± 607.5861856809066"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 8579347.498697916,
            "unit": "ns",
            "range": "± 30455.395291070854"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 253830.77210286458,
            "unit": "ns",
            "range": "± 1694.9094762137734"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 138076.61945452009,
            "unit": "ns",
            "range": "± 661.5428657986412"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 18751.246871948242,
            "unit": "ns",
            "range": "± 53.22142843942405"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 16782.839447021484,
            "unit": "ns",
            "range": "± 11.251429149782897"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 145681.66520808294,
            "unit": "ns",
            "range": "± 347.415260893733"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 44827.85924588717,
            "unit": "ns",
            "range": "± 14.949152236105101"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 126703.58494059245,
            "unit": "ns",
            "range": "± 79.97332927618069"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 9609905.17299107,
            "unit": "ns",
            "range": "± 48064.073232886825"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 275155.8475167411,
            "unit": "ns",
            "range": "± 685.8381252075528"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 141251.63985770088,
            "unit": "ns",
            "range": "± 516.691014013005"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 18708.394585164388,
            "unit": "ns",
            "range": "± 16.12915785287024"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 16733.307391826922,
            "unit": "ns",
            "range": "± 8.506507281495274"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 145866.6792689732,
            "unit": "ns",
            "range": "± 387.3077740549369"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 45695.10449829102,
            "unit": "ns",
            "range": "± 97.96439983614657"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 129211.841015625,
            "unit": "ns",
            "range": "± 543.1204760458534"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 9482242.169471154,
            "unit": "ns",
            "range": "± 34838.00316173637"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 279406.8555036272,
            "unit": "ns",
            "range": "± 810.6491395314258"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744675918856,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 144733.75927734375,
            "unit": "ns",
            "range": "± 611.116942482136"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 18372.612171718054,
            "unit": "ns",
            "range": "± 196.86487466585208"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 16632.63051554362,
            "unit": "ns",
            "range": "± 145.82711855603782"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 153622.2749186198,
            "unit": "ns",
            "range": "± 1315.7541336702923"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 46743.350661057695,
            "unit": "ns",
            "range": "± 153.4193609989364"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 138511.49665715144,
            "unit": "ns",
            "range": "± 494.4038894934692"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 10600114.625,
            "unit": "ns",
            "range": "± 196726.40006709757"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 292721.4490206069,
            "unit": "ns",
            "range": "± 14081.423597239325"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 143600.34639485678,
            "unit": "ns",
            "range": "± 399.38841029642884"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 19112.3095304049,
            "unit": "ns",
            "range": "± 52.81591247322558"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 16627.22867635091,
            "unit": "ns",
            "range": "± 139.23941119168109"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 144564.68575345553,
            "unit": "ns",
            "range": "± 180.66932780657552"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 45512.87201131185,
            "unit": "ns",
            "range": "± 385.98927242800056"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 134949.0056966146,
            "unit": "ns",
            "range": "± 451.4976675599588"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 10737839.646875,
            "unit": "ns",
            "range": "± 198459.8651311298"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 278974.6994194135,
            "unit": "ns",
            "range": "± 13870.9477490921"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 145070.7958984375,
            "unit": "ns",
            "range": "± 1282.2258695852022"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 18382.518556867326,
            "unit": "ns",
            "range": "± 100.95059341657455"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 16778.915267944336,
            "unit": "ns",
            "range": "± 24.324272978121282"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 146339.5724609375,
            "unit": "ns",
            "range": "± 1339.0729546348093"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 46512.57604980469,
            "unit": "ns",
            "range": "± 257.10221879604484"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 133757.98035606972,
            "unit": "ns",
            "range": "± 740.604152782528"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 8718114.838942308,
            "unit": "ns",
            "range": "± 43839.599393821074"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 250882.22517903647,
            "unit": "ns",
            "range": "± 599.3579654978159"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 142183.8868815104,
            "unit": "ns",
            "range": "± 830.2283029940837"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 18171.16352589925,
            "unit": "ns",
            "range": "± 36.00807563753736"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 16715.65269470215,
            "unit": "ns",
            "range": "± 16.031586268644457"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 144945.96287434894,
            "unit": "ns",
            "range": "± 1018.4324567381751"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 46979.46951998197,
            "unit": "ns",
            "range": "± 56.358657922618725"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 136587.99676044172,
            "unit": "ns",
            "range": "± 594.2516006664216"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 9528540.507211538,
            "unit": "ns",
            "range": "± 26262.675999577765"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 272753.5975585937,
            "unit": "ns",
            "range": "± 1961.482615934721"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 145577.39454752606,
            "unit": "ns",
            "range": "± 1026.933631739176"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 19095.108923339845,
            "unit": "ns",
            "range": "± 116.47138147618135"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 16656.20187886556,
            "unit": "ns",
            "range": "± 11.082625276692747"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 147144.75463053386,
            "unit": "ns",
            "range": "± 1088.8606981034495"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 47132.431828425484,
            "unit": "ns",
            "range": "± 42.33193750377442"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 132899.18408203125,
            "unit": "ns",
            "range": "± 136.10496367262277"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 9663894.8984375,
            "unit": "ns",
            "range": "± 62865.77004622018"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 278106.08458533656,
            "unit": "ns",
            "range": "± 1142.8774051956852"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770250534,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 142765.3686174665,
            "unit": "ns",
            "range": "± 750.7598961836995"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 19182.308752877372,
            "unit": "ns",
            "range": "± 117.21943743533214"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 16607.98950631278,
            "unit": "ns",
            "range": "± 39.20729714173981"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 144644.13792201452,
            "unit": "ns",
            "range": "± 480.44718182691804"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 46461.25783793131,
            "unit": "ns",
            "range": "± 46.8250208646076"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 139521.36347307477,
            "unit": "ns",
            "range": "± 342.68770208391715"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 10655184.1,
            "unit": "ns",
            "range": "± 163162.77021242896"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 277923.48509457236,
            "unit": "ns",
            "range": "± 14076.085682362078"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 142792.72376427284,
            "unit": "ns",
            "range": "± 552.3183040499698"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 19046.518565586633,
            "unit": "ns",
            "range": "± 139.96018936722282"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 16581.053749084473,
            "unit": "ns",
            "range": "± 29.35827667979447"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 141841.13016451322,
            "unit": "ns",
            "range": "± 297.8246090985638"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 45466.653904506136,
            "unit": "ns",
            "range": "± 171.1213627352437"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 136979.99822998047,
            "unit": "ns",
            "range": "± 126.94946269947381"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 10787157.57700893,
            "unit": "ns",
            "range": "± 122200.79837502672"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 283659.57515656,
            "unit": "ns",
            "range": "± 12961.879581517898"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 144180.56824669472,
            "unit": "ns",
            "range": "± 526.0702395284476"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 18743.908153827375,
            "unit": "ns",
            "range": "± 17.412363651534655"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 16856.92547200521,
            "unit": "ns",
            "range": "± 130.36381813626244"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 146077.35514322916,
            "unit": "ns",
            "range": "± 198.55699388625672"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 47125.567962646484,
            "unit": "ns",
            "range": "± 97.10638384110436"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 135246.16314227766,
            "unit": "ns",
            "range": "± 280.8499496567579"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 8691883.520432692,
            "unit": "ns",
            "range": "± 31470.75832713524"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 252399.64481026787,
            "unit": "ns",
            "range": "± 386.1184615205976"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 144965.92196219307,
            "unit": "ns",
            "range": "± 1221.8986229886702"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 18093.30992780413,
            "unit": "ns",
            "range": "± 52.09100788207147"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 16701.93393351237,
            "unit": "ns",
            "range": "± 129.18532411648656"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 143474.29895958534,
            "unit": "ns",
            "range": "± 212.1459527357954"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 47415.857922363284,
            "unit": "ns",
            "range": "± 232.59732897525336"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 135491.03803710936,
            "unit": "ns",
            "range": "± 670.1808946362224"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 9652436.76674107,
            "unit": "ns",
            "range": "± 65129.870590890954"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 273638.01896784856,
            "unit": "ns",
            "range": "± 846.2985142744611"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 144396.43118489583,
            "unit": "ns",
            "range": "± 824.6777358072417"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 18301.762393657977,
            "unit": "ns",
            "range": "± 30.415963294127195"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 16604.47428588867,
            "unit": "ns",
            "range": "± 102.9544741233238"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 145920.204296875,
            "unit": "ns",
            "range": "± 1179.52242561969"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 46662.44626464844,
            "unit": "ns",
            "range": "± 161.32874286898598"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 135129.45122419085,
            "unit": "ns",
            "range": "± 260.90185091729063"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 9657326.618990384,
            "unit": "ns",
            "range": "± 51080.899899551056"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 276173.94028320315,
            "unit": "ns",
            "range": "± 1852.7971190023804"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744675996654,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 14568.831507364908,
            "unit": "ns",
            "range": "± 17.162867138062918"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 19178.018842424666,
            "unit": "ns",
            "range": "± 35.24813923766544"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 20843.85702950614,
            "unit": "ns",
            "range": "± 48.023024023320836"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 22025.21010178786,
            "unit": "ns",
            "range": "± 33.69690488196648"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15912.472182053785,
            "unit": "ns",
            "range": "± 53.10096191826291"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 9742.411158635066,
            "unit": "ns",
            "range": "± 15.460885241174669"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 20637.197977701824,
            "unit": "ns",
            "range": "± 33.80657194144706"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 20510.45872614934,
            "unit": "ns",
            "range": "± 21.497665273224687"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 25256.03572300502,
            "unit": "ns",
            "range": "± 146.81308937275816"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 25216.04715983073,
            "unit": "ns",
            "range": "± 118.7927336457878"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 19191.149030412947,
            "unit": "ns",
            "range": "± 51.13203537336046"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 25443.504130045574,
            "unit": "ns",
            "range": "± 73.59452520147713"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 27826.659545898438,
            "unit": "ns",
            "range": "± 77.98883483634013"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 28182.7143351237,
            "unit": "ns",
            "range": "± 93.2439247471165"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15331.562805175781,
            "unit": "ns",
            "range": "± 16.603269003851402"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10118.698354867789,
            "unit": "ns",
            "range": "± 19.396153131674648"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 27434.89715576172,
            "unit": "ns",
            "range": "± 60.685943518294884"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 26758.446553548176,
            "unit": "ns",
            "range": "± 102.45141861026603"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 31593.258870442707,
            "unit": "ns",
            "range": "± 126.8534581387131"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 32272.749430338543,
            "unit": "ns",
            "range": "± 74.85021753190283"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 13617.684819148137,
            "unit": "ns",
            "range": "± 13.850789317930081"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19240.638296944755,
            "unit": "ns",
            "range": "± 36.76807370997342"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 20087.19259408804,
            "unit": "ns",
            "range": "± 17.63930403855747"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 21165.421588604266,
            "unit": "ns",
            "range": "± 37.45391463155143"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15613.949584960938,
            "unit": "ns",
            "range": "± 38.30867436538728"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 9802.939932686942,
            "unit": "ns",
            "range": "± 12.905453597927288"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 19857.29217529297,
            "unit": "ns",
            "range": "± 14.706189491751172"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 23173.878377278645,
            "unit": "ns",
            "range": "± 32.15739799937907"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 24774.000549316406,
            "unit": "ns",
            "range": "± 31.214990669194393"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 28721.163126627605,
            "unit": "ns",
            "range": "± 46.725316164727126"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770303214,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 13925.361684163412,
            "unit": "ns",
            "range": "± 13.693712975130754"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 19831.127711704798,
            "unit": "ns",
            "range": "± 132.6725286425521"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 19919.585309709822,
            "unit": "ns",
            "range": "± 29.208067692742127"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 22364.57766019381,
            "unit": "ns",
            "range": "± 43.08054487900734"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15610.379682268414,
            "unit": "ns",
            "range": "± 20.97332038991457"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 9553.909410749164,
            "unit": "ns",
            "range": "± 10.425887972670287"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 20508.809955303484,
            "unit": "ns",
            "range": "± 33.06239585714511"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 21696.13026936849,
            "unit": "ns",
            "range": "± 27.7047473443294"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 25044.239400227863,
            "unit": "ns",
            "range": "± 86.08764306414146"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 25761.098225911457,
            "unit": "ns",
            "range": "± 68.75343351594236"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 19216.61638532366,
            "unit": "ns",
            "range": "± 66.87426712470847"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 26553.06596022386,
            "unit": "ns",
            "range": "± 60.84362741870182"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 25394.824625651043,
            "unit": "ns",
            "range": "± 51.185035116601334"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 30296.509806315105,
            "unit": "ns",
            "range": "± 83.0694007528121"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15131.968219463643,
            "unit": "ns",
            "range": "± 15.994944541654808"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 11307.651754525992,
            "unit": "ns",
            "range": "± 14.98487382441633"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 26786.573573521204,
            "unit": "ns",
            "range": "± 37.96323372289569"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 28008.01074688251,
            "unit": "ns",
            "range": "± 42.92859978868862"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 30354.025472005207,
            "unit": "ns",
            "range": "± 95.52010217985439"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 30976.055472237724,
            "unit": "ns",
            "range": "± 87.39104016169652"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 13787.911987304688,
            "unit": "ns",
            "range": "± 11.227687324604828"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19329.83154296875,
            "unit": "ns",
            "range": "± 53.51531530604885"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 21065.221913655598,
            "unit": "ns",
            "range": "± 40.714759082932176"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 22084.122794015067,
            "unit": "ns",
            "range": "± 20.4756807067216"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15523.03444998605,
            "unit": "ns",
            "range": "± 22.197539575101214"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 9760.215214320591,
            "unit": "ns",
            "range": "± 21.93682252338772"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 20012.800816127234,
            "unit": "ns",
            "range": "± 22.60816955072468"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 20519.886169433594,
            "unit": "ns",
            "range": "± 34.113056167485496"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 28248.572692871094,
            "unit": "ns",
            "range": "± 46.24180564794525"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 27306.416727701824,
            "unit": "ns",
            "range": "± 71.12262209055591"
          }
        ]
      }
    ],
    "Operations.RawStringOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744676018608,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 14385.007680257162,
            "unit": "ns",
            "range": "± 16.56942239545918"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 20426.752061110277,
            "unit": "ns",
            "range": "± 36.38927629763169"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 20740.72021484375,
            "unit": "ns",
            "range": "± 60.217157961243295"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 21437.9392183744,
            "unit": "ns",
            "range": "± 26.110725303350478"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 16206.695556640625,
            "unit": "ns",
            "range": "± 16.682538447427635"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 11293.07144165039,
            "unit": "ns",
            "range": "± 35.09724473567952"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 22205.75185139974,
            "unit": "ns",
            "range": "± 114.34641690015124"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 22098.489732008715,
            "unit": "ns",
            "range": "± 43.87990629189011"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 26210.080660306492,
            "unit": "ns",
            "range": "± 36.25016881079096"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 27099.983723958332,
            "unit": "ns",
            "range": "± 74.25723526201558"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 20399.486490885418,
            "unit": "ns",
            "range": "± 67.5483744289252"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 25965.25421142578,
            "unit": "ns",
            "range": "± 39.87906474412965"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 26793.526567731584,
            "unit": "ns",
            "range": "± 61.066891377120974"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 27500.07080078125,
            "unit": "ns",
            "range": "± 124.20448481580786"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15359.270833333334,
            "unit": "ns",
            "range": "± 17.82245325585702"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10848.870631626674,
            "unit": "ns",
            "range": "± 14.01445784644893"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 27384.864196777344,
            "unit": "ns",
            "range": "± 29.292833440028943"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 27391.913045247395,
            "unit": "ns",
            "range": "± 52.087436628206405"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 32766.475568498885,
            "unit": "ns",
            "range": "± 87.24430675276203"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 33261.94545200893,
            "unit": "ns",
            "range": "± 109.18798219233737"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14362.155369349888,
            "unit": "ns",
            "range": "± 26.370073725703154"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 19863.031475360578,
            "unit": "ns",
            "range": "± 32.76912219023978"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 21285.450157752402,
            "unit": "ns",
            "range": "± 44.5418441343957"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 20881.588527134485,
            "unit": "ns",
            "range": "± 45.94756690049944"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 16010.020243326822,
            "unit": "ns",
            "range": "± 217.64301653385488"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10948.9872272198,
            "unit": "ns",
            "range": "± 14.560550600707524"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 21485.594059870793,
            "unit": "ns",
            "range": "± 23.92286177798289"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 22308.677673339844,
            "unit": "ns",
            "range": "± 40.46807066757511"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 26614.10380045573,
            "unit": "ns",
            "range": "± 143.74339005326712"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 26602.1381632487,
            "unit": "ns",
            "range": "± 88.85407714063145"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770327248,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: ACL)",
            "value": 13868.40329851423,
            "unit": "ns",
            "range": "± 47.40437888382707"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: ACL)",
            "value": 19753.786141531808,
            "unit": "ns",
            "range": "± 21.437290484263407"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: ACL)",
            "value": 21824.640764508928,
            "unit": "ns",
            "range": "± 37.29607120068492"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: ACL)",
            "value": 21225.587209065754,
            "unit": "ns",
            "range": "± 27.499597774642734"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: ACL)",
            "value": 15679.77283184345,
            "unit": "ns",
            "range": "± 16.09615811056543"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: ACL)",
            "value": 10721.730143229166,
            "unit": "ns",
            "range": "± 16.317814755124974"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: ACL)",
            "value": 21640.274869478664,
            "unit": "ns",
            "range": "± 19.40807511942728"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: ACL)",
            "value": 21631.542256673176,
            "unit": "ns",
            "range": "± 40.472578554105475"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: ACL)",
            "value": 28003.64728655134,
            "unit": "ns",
            "range": "± 90.44036779868617"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: ACL)",
            "value": 26043.751408503605,
            "unit": "ns",
            "range": "± 28.57921747547119"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: AOF)",
            "value": 19691.800580705916,
            "unit": "ns",
            "range": "± 46.11361382751021"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: AOF)",
            "value": 25857.188633510046,
            "unit": "ns",
            "range": "± 48.49963713198006"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: AOF)",
            "value": 26747.788899739582,
            "unit": "ns",
            "range": "± 49.08343657514605"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: AOF)",
            "value": 26909.622701009113,
            "unit": "ns",
            "range": "± 67.38077441648521"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: AOF)",
            "value": 15805.260721842447,
            "unit": "ns",
            "range": "± 12.757869351491175"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: AOF)",
            "value": 10903.074645996094,
            "unit": "ns",
            "range": "± 12.793190331760469"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: AOF)",
            "value": 26457.324872698104,
            "unit": "ns",
            "range": "± 37.450999487828796"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: AOF)",
            "value": 26221.560770670574,
            "unit": "ns",
            "range": "± 38.15543202567123"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: AOF)",
            "value": 31396.055501302082,
            "unit": "ns",
            "range": "± 159.66510267617068"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: AOF)",
            "value": 30655.026245117188,
            "unit": "ns",
            "range": "± 106.474447604089"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Set(Params: None)",
            "value": 14465.287722074068,
            "unit": "ns",
            "range": "± 22.52275584112097"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetEx(Params: None)",
            "value": 20025.348409016926,
            "unit": "ns",
            "range": "± 28.15314246041538"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetNx(Params: None)",
            "value": 20985.584411621094,
            "unit": "ns",
            "range": "± 25.997036129373836"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.SetXx(Params: None)",
            "value": 21115.29780796596,
            "unit": "ns",
            "range": "± 33.92293887798946"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetFound(Params: None)",
            "value": 15542.015482584635,
            "unit": "ns",
            "range": "± 19.580676442585567"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.GetNotFound(Params: None)",
            "value": 10757.098032633463,
            "unit": "ns",
            "range": "± 24.212075821706353"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Increment(Params: None)",
            "value": 21434.834507533484,
            "unit": "ns",
            "range": "± 31.963455138420684"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.Decrement(Params: None)",
            "value": 21487.773786272322,
            "unit": "ns",
            "range": "± 22.34907398709295"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.IncrementBy(Params: None)",
            "value": 26118.298950195312,
            "unit": "ns",
            "range": "± 89.09373025042869"
          },
          {
            "name": "BDN.benchmark.Operations.RawStringOperations.DecrementBy(Params: None)",
            "value": 25416.52374267578,
            "unit": "ns",
            "range": "± 63.533308509757276"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744676155116,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 103534.6070992606,
            "unit": "ns",
            "range": "± 652.3489871839957"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 11462.76557413737,
            "unit": "ns",
            "range": "± 54.97771628692179"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11397.797282627651,
            "unit": "ns",
            "range": "± 33.56915006321829"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10376.934469369742,
            "unit": "ns",
            "range": "± 20.762778457421284"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 12676.745238084059,
            "unit": "ns",
            "range": "± 23.80559988554652"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 13279.959742736817,
            "unit": "ns",
            "range": "± 50.10549667382174"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 11452.77547302246,
            "unit": "ns",
            "range": "± 58.443597861397016"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10328.337802342006,
            "unit": "ns",
            "range": "± 56.306165548315214"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 12874.974624633789,
            "unit": "ns",
            "range": "± 129.7427584533525"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13385.713627741887,
            "unit": "ns",
            "range": "± 26.53287050953307"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 11399.940451988807,
            "unit": "ns",
            "range": "± 30.491762483583067"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 5354.214723205567,
            "unit": "ns",
            "range": "± 27.665086335314953"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12698.08200945173,
            "unit": "ns",
            "range": "± 37.49597227231384"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 12202.821110534667,
            "unit": "ns",
            "range": "± 54.402839324054426"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 11688.870624542236,
            "unit": "ns",
            "range": "± 14.25465081100574"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 122341.2741793119,
            "unit": "ns",
            "range": "± 371.6637809494421"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 48897.55630493164,
            "unit": "ns",
            "range": "± 143.9386690377281"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 51651.013466971264,
            "unit": "ns",
            "range": "± 261.5138520321975"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 56962.371224539624,
            "unit": "ns",
            "range": "± 148.73719950031153"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 65117.3753133138,
            "unit": "ns",
            "range": "± 536.6323034216784"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 94472.49069010417,
            "unit": "ns",
            "range": "± 602.5922727946357"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 56530.993670654294,
            "unit": "ns",
            "range": "± 167.38802849199794"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 48199.672839355466,
            "unit": "ns",
            "range": "± 215.1742460035104"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 60437.0628112793,
            "unit": "ns",
            "range": "± 401.1118565639311"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 68367.04243977864,
            "unit": "ns",
            "range": "± 464.77786603220284"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 64226.1178119366,
            "unit": "ns",
            "range": "± 354.079124112713"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 5291.952906799316,
            "unit": "ns",
            "range": "± 25.04278987472293"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 57127.55643513997,
            "unit": "ns",
            "range": "± 253.76754943212597"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 51629.72645670573,
            "unit": "ns",
            "range": "± 184.0423892476155"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 54139.505446370444,
            "unit": "ns",
            "range": "± 158.66391964719082"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 102752.9528564453,
            "unit": "ns",
            "range": "± 1252.3756254313935"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 52858.363635723406,
            "unit": "ns",
            "range": "± 230.58920040083083"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 53108.10405069987,
            "unit": "ns",
            "range": "± 186.10494023999922"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 57172.0273050944,
            "unit": "ns",
            "range": "± 168.46963600555318"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 59308.983498128255,
            "unit": "ns",
            "range": "± 186.6016479634247"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 82137.54443359375,
            "unit": "ns",
            "range": "± 172.07203559693266"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 54312.87376912435,
            "unit": "ns",
            "range": "± 42.388230912593016"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 46631.5332010905,
            "unit": "ns",
            "range": "± 165.3662294413269"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 57500.6663655599,
            "unit": "ns",
            "range": "± 153.93879008602244"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 57286.170798165454,
            "unit": "ns",
            "range": "± 267.0696161763171"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 60835.99201863607,
            "unit": "ns",
            "range": "± 120.14046707218215"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 5316.003420148577,
            "unit": "ns",
            "range": "± 6.848545287416645"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 49373.28022257487,
            "unit": "ns",
            "range": "± 107.46384099585966"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 52722.580401611325,
            "unit": "ns",
            "range": "± 123.39502482640484"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 56601.849351149336,
            "unit": "ns",
            "range": "± 116.92288830140329"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770534416,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 103571.5241455078,
            "unit": "ns",
            "range": "± 559.4400906543178"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 11390.40983835856,
            "unit": "ns",
            "range": "± 11.911437681006836"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11489.572944641113,
            "unit": "ns",
            "range": "± 53.71591359355221"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10292.046018160307,
            "unit": "ns",
            "range": "± 6.217772785876957"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 12765.039652143207,
            "unit": "ns",
            "range": "± 44.87081000046937"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 13106.471747080484,
            "unit": "ns",
            "range": "± 13.755649758851222"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 11335.72777557373,
            "unit": "ns",
            "range": "± 34.33316120767439"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10142.33794621059,
            "unit": "ns",
            "range": "± 38.85296425321717"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 12862.70255279541,
            "unit": "ns",
            "range": "± 48.81866780695491"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13450.40284423828,
            "unit": "ns",
            "range": "± 46.795976874593734"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 11418.253457641602,
            "unit": "ns",
            "range": "± 50.522025260648284"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 5459.951275126139,
            "unit": "ns",
            "range": "± 18.173441522178518"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12785.389027913412,
            "unit": "ns",
            "range": "± 66.75961767737897"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 12332.705660893367,
            "unit": "ns",
            "range": "± 47.33800607132569"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 11479.806256103515,
            "unit": "ns",
            "range": "± 44.5384058907674"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 122789.32150878906,
            "unit": "ns",
            "range": "± 926.7204362129371"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 50162.344490559895,
            "unit": "ns",
            "range": "± 217.98741676836437"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 56407.48173740932,
            "unit": "ns",
            "range": "± 148.40171231773797"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 55916.86790974935,
            "unit": "ns",
            "range": "± 211.82585904339138"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 66874.36103108725,
            "unit": "ns",
            "range": "± 259.4383503142164"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 93381.09604317801,
            "unit": "ns",
            "range": "± 453.8552449714124"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 55010.207523600264,
            "unit": "ns",
            "range": "± 196.8923181012248"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 46860.35187639509,
            "unit": "ns",
            "range": "± 76.04841238082459"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 63430.237407977766,
            "unit": "ns",
            "range": "± 154.4386122555712"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 71349.14243977865,
            "unit": "ns",
            "range": "± 305.0195721671901"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 61089.68743693034,
            "unit": "ns",
            "range": "± 326.86077163961505"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 5328.191464233399,
            "unit": "ns",
            "range": "± 24.74224667317683"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 57657.97888183594,
            "unit": "ns",
            "range": "± 199.93912735674763"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 50695.72174275716,
            "unit": "ns",
            "range": "± 156.07458384241804"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 56767.52177647182,
            "unit": "ns",
            "range": "± 181.14332430959197"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 101033.04404296874,
            "unit": "ns",
            "range": "± 600.9405377699926"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 49176.19469401042,
            "unit": "ns",
            "range": "± 111.93778755922958"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 52443.1593976702,
            "unit": "ns",
            "range": "± 207.63834462489925"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 55915.16645595006,
            "unit": "ns",
            "range": "± 45.05095867509431"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 61068.84680175781,
            "unit": "ns",
            "range": "± 124.570765260445"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 82971.19635881696,
            "unit": "ns",
            "range": "± 480.72343067950897"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 55805.53764851888,
            "unit": "ns",
            "range": "± 153.18572317799934"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 46604.01219395229,
            "unit": "ns",
            "range": "± 219.00128035172202"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 57974.7291128976,
            "unit": "ns",
            "range": "± 142.03766151830305"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 58993.545517985025,
            "unit": "ns",
            "range": "± 197.09089664910508"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 61309.89423624674,
            "unit": "ns",
            "range": "± 100.3491903383616"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 5368.199782235281,
            "unit": "ns",
            "range": "± 17.60708221848814"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 50964.12991536458,
            "unit": "ns",
            "range": "± 135.91420644865636"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 52780.050248209634,
            "unit": "ns",
            "range": "± 186.05076071554822"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 55260.22690226237,
            "unit": "ns",
            "range": "± 159.29902593016942"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744676157247,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 140822.05970982142,
            "unit": "ns",
            "range": "± 392.79387193900294"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 11429.28700205485,
            "unit": "ns",
            "range": "± 73.15676048107312"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11553.492239815849,
            "unit": "ns",
            "range": "± 69.33833711174435"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10194.432009379068,
            "unit": "ns",
            "range": "± 7.326237397894173"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 12581.410572306315,
            "unit": "ns",
            "range": "± 75.65528829758615"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 13443.554727172852,
            "unit": "ns",
            "range": "± 87.33365929232781"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 13111.114930470785,
            "unit": "ns",
            "range": "± 8.69368243283471"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10082.394524207482,
            "unit": "ns",
            "range": "± 17.8005623081349"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 12764.23996073405,
            "unit": "ns",
            "range": "± 79.11370513364096"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13277.115524291992,
            "unit": "ns",
            "range": "± 45.718883539072486"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 11564.849047342936,
            "unit": "ns",
            "range": "± 13.490723686021777"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 13683.77272578648,
            "unit": "ns",
            "range": "± 42.19323396930201"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12692.969618577223,
            "unit": "ns",
            "range": "± 11.426996192854036"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 12096.223649245043,
            "unit": "ns",
            "range": "± 35.7499886209033"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 13222.51253560384,
            "unit": "ns",
            "range": "± 91.57230773518337"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 160234.35353190106,
            "unit": "ns",
            "range": "± 1073.0076711948075"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 62648.06953938802,
            "unit": "ns",
            "range": "± 303.91168658701724"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 48536.833666120256,
            "unit": "ns",
            "range": "± 133.27399837958376"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 53085.196645883414,
            "unit": "ns",
            "range": "± 79.08419686631427"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 83275.82887369792,
            "unit": "ns",
            "range": "± 344.3954200626593"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 115064.65975516183,
            "unit": "ns",
            "range": "± 433.89382493367805"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 53254.45428873698,
            "unit": "ns",
            "range": "± 246.65604456521137"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 53747.152267456055,
            "unit": "ns",
            "range": "± 70.13069921478534"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 54377.57553100586,
            "unit": "ns",
            "range": "± 393.71071094531334"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 87928.56946614584,
            "unit": "ns",
            "range": "± 558.2748158097633"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 63038.94080461775,
            "unit": "ns",
            "range": "± 311.79714689248044"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 13304.03588511149,
            "unit": "ns",
            "range": "± 40.501778675055185"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 77617.96371895926,
            "unit": "ns",
            "range": "± 243.9831046688959"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 59031.78404541015,
            "unit": "ns",
            "range": "± 186.73302858973773"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 50584.1553141276,
            "unit": "ns",
            "range": "± 407.7880943671151"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 138262.99270833333,
            "unit": "ns",
            "range": "± 945.3122353070411"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 62539.352848307295,
            "unit": "ns",
            "range": "± 389.04978639352476"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 47871.55677693685,
            "unit": "ns",
            "range": "± 270.64526409061534"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 50734.892294747486,
            "unit": "ns",
            "range": "± 265.60111781942237"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 82555.56317608173,
            "unit": "ns",
            "range": "± 225.28182527167758"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 103903.2497907366,
            "unit": "ns",
            "range": "± 357.4107455334162"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 54126.28260149275,
            "unit": "ns",
            "range": "± 191.1162932297141"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 52605.37576293945,
            "unit": "ns",
            "range": "± 144.92554907242464"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 51553.510762532555,
            "unit": "ns",
            "range": "± 236.49939825389217"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 80877.77927943638,
            "unit": "ns",
            "range": "± 193.40925001967958"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 58867.18573404948,
            "unit": "ns",
            "range": "± 299.09326309595474"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 13870.277628217425,
            "unit": "ns",
            "range": "± 36.38160827304273"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 68797.40295410156,
            "unit": "ns",
            "range": "± 189.22349062219521"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 58504.44233398438,
            "unit": "ns",
            "range": "± 183.37510983524393"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 48403.03120204381,
            "unit": "ns",
            "range": "± 96.64944090185436"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770505836,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 139783.90826822916,
            "unit": "ns",
            "range": "± 917.4130270718244"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 11445.160975138346,
            "unit": "ns",
            "range": "± 36.48784494408705"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11449.339127760668,
            "unit": "ns",
            "range": "± 33.416553214384244"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10279.761786905925,
            "unit": "ns",
            "range": "± 117.60649950857372"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 12586.940098353794,
            "unit": "ns",
            "range": "± 56.94442838978597"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 13494.777066040038,
            "unit": "ns",
            "range": "± 86.22038438344111"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 12540.016115315755,
            "unit": "ns",
            "range": "± 64.60343789943633"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10163.398041861397,
            "unit": "ns",
            "range": "± 36.00749018090602"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 12680.895790686975,
            "unit": "ns",
            "range": "± 55.5395193226932"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13167.059518178305,
            "unit": "ns",
            "range": "± 9.417103494615635"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 11682.595029558453,
            "unit": "ns",
            "range": "± 32.78706755531922"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 13681.922580174038,
            "unit": "ns",
            "range": "± 53.5235373886797"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12680.542616707939,
            "unit": "ns",
            "range": "± 26.40963444593771"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 12074.11839090983,
            "unit": "ns",
            "range": "± 59.292468650968026"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 13799.290506635394,
            "unit": "ns",
            "range": "± 9.473571766152974"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 154304.57229003907,
            "unit": "ns",
            "range": "± 1106.3416923571983"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 62601.260838099886,
            "unit": "ns",
            "range": "± 376.7686040347138"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 47586.73318716196,
            "unit": "ns",
            "range": "± 161.53299651014933"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 51123.37990722656,
            "unit": "ns",
            "range": "± 243.99306839991328"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 86055.16833496094,
            "unit": "ns",
            "range": "± 442.54588232793094"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 117719.56164550781,
            "unit": "ns",
            "range": "± 707.310401755265"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 49837.311535644534,
            "unit": "ns",
            "range": "± 223.3261507937355"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 53833.982161301836,
            "unit": "ns",
            "range": "± 112.92090725683421"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 53719.019962855746,
            "unit": "ns",
            "range": "± 252.1146585148154"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 88793.67830984933,
            "unit": "ns",
            "range": "± 356.5262583857577"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 59623.385201590405,
            "unit": "ns",
            "range": "± 220.1541790125721"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 13815.556705729166,
            "unit": "ns",
            "range": "± 40.11975402786217"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 75987.37698567708,
            "unit": "ns",
            "range": "± 377.54182673073626"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 58011.051928710935,
            "unit": "ns",
            "range": "± 232.30592905053254"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 50534.89050292969,
            "unit": "ns",
            "range": "± 204.7806213770644"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 142148.65415039062,
            "unit": "ns",
            "range": "± 768.3624854989725"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 57823.8124613444,
            "unit": "ns",
            "range": "± 253.8873496963747"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 47636.61560465495,
            "unit": "ns",
            "range": "± 302.70188846058784"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 52067.169529506136,
            "unit": "ns",
            "range": "± 189.0460441600601"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 76538.86392415364,
            "unit": "ns",
            "range": "± 335.43316510230153"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 104582.38509695871,
            "unit": "ns",
            "range": "± 255.5952942438311"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 55712.37995692662,
            "unit": "ns",
            "range": "± 89.57807241295414"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 57983.09024454753,
            "unit": "ns",
            "range": "± 264.2227180694921"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 53598.234851074216,
            "unit": "ns",
            "range": "± 168.00608964117262"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 78122.9382405599,
            "unit": "ns",
            "range": "± 238.63504544075724"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 64426.21722819011,
            "unit": "ns",
            "range": "± 242.44106350610264"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 13190.292547607422,
            "unit": "ns",
            "range": "± 25.061198599854755"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 70788.47171223958,
            "unit": "ns",
            "range": "± 245.22093096454188"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 57879.83584798177,
            "unit": "ns",
            "range": "± 262.8134300491133"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 54189.64889322917,
            "unit": "ns",
            "range": "± 154.63099900582682"
          }
        ]
      }
    ],
    "Operations.SetOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744676226353,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 121741.13439941406,
            "unit": "ns",
            "range": "± 1926.7609214704787"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 61240.84146352915,
            "unit": "ns",
            "range": "± 201.35535734169713"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10356.671634928385,
            "unit": "ns",
            "range": "± 65.08289731164366"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 11386.27875300816,
            "unit": "ns",
            "range": "± 61.40524764384127"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 26245.9412109375,
            "unit": "ns",
            "range": "± 149.69986160325763"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 12237.978205754207,
            "unit": "ns",
            "range": "± 23.032766391554926"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 13898.0790612357,
            "unit": "ns",
            "range": "± 64.47006211754086"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 12108.232443002555,
            "unit": "ns",
            "range": "± 10.67143205788294"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 11409.62885945638,
            "unit": "ns",
            "range": "± 57.82278568131174"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 12468.78968556722,
            "unit": "ns",
            "range": "± 62.96020497531513"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 13773.636372157505,
            "unit": "ns",
            "range": "± 63.314593706228514"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 12758.510056715746,
            "unit": "ns",
            "range": "± 37.02185076041154"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 14119.77077331543,
            "unit": "ns",
            "range": "± 85.47839248687451"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 13939.989725748697,
            "unit": "ns",
            "range": "± 6.2267887773462824"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 12357.17187147874,
            "unit": "ns",
            "range": "± 13.108433374249437"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 13340.04149983724,
            "unit": "ns",
            "range": "± 64.14947413950699"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 132422.89961751303,
            "unit": "ns",
            "range": "± 788.1533941107738"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 138145.3110514323,
            "unit": "ns",
            "range": "± 1088.0708446152512"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 46894.726693289624,
            "unit": "ns",
            "range": "± 163.9933038027508"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 53571.164123535156,
            "unit": "ns",
            "range": "± 152.57845009101413"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 251611.49780273438,
            "unit": "ns",
            "range": "± 2709.1132249708958"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 55397.283231608075,
            "unit": "ns",
            "range": "± 247.48583359657292"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 58954.10119628906,
            "unit": "ns",
            "range": "± 289.20512860523115"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 61990.98849487305,
            "unit": "ns",
            "range": "± 89.88299825110606"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 67443.8486328125,
            "unit": "ns",
            "range": "± 436.0623615009393"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 159819.33208356585,
            "unit": "ns",
            "range": "± 959.7322523014656"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 236133.96748046874,
            "unit": "ns",
            "range": "± 1444.8415863088217"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 153179.9711263021,
            "unit": "ns",
            "range": "± 1284.4390758899142"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 229548.11490885416,
            "unit": "ns",
            "range": "± 2236.710095843463"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 152569.78491210938,
            "unit": "ns",
            "range": "± 677.7363923164771"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 157944.96848958332,
            "unit": "ns",
            "range": "± 1359.2495183300566"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 230602.98767903645,
            "unit": "ns",
            "range": "± 1216.6148982401985"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 120477.24771554129,
            "unit": "ns",
            "range": "± 977.2752641191526"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 131701.8155843099,
            "unit": "ns",
            "range": "± 1187.2564486376962"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 44754.56055704752,
            "unit": "ns",
            "range": "± 249.85882404715508"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 54377.661657714845,
            "unit": "ns",
            "range": "± 216.2993084489542"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 241463.28767903647,
            "unit": "ns",
            "range": "± 1176.2406512441464"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 55265.65897623698,
            "unit": "ns",
            "range": "± 103.51102964893333"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 60876.082266671314,
            "unit": "ns",
            "range": "± 192.46917915762387"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 59643.12875773112,
            "unit": "ns",
            "range": "± 157.0801372992109"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 66235.11649733323,
            "unit": "ns",
            "range": "± 192.30698201480837"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 149535.68855794272,
            "unit": "ns",
            "range": "± 575.0904907660439"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 198267.20989583334,
            "unit": "ns",
            "range": "± 1225.1021891220082"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 145776.68231608073,
            "unit": "ns",
            "range": "± 600.6347116822195"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 189192.30800083705,
            "unit": "ns",
            "range": "± 565.1726963351732"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 139828.9931265024,
            "unit": "ns",
            "range": "± 199.23636947221317"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 145548.4035970052,
            "unit": "ns",
            "range": "± 566.9632161597686"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 193009.5468343099,
            "unit": "ns",
            "range": "± 642.4734649522148"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770592056,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 124084.6333984375,
            "unit": "ns",
            "range": "± 1815.767418241644"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 61646.396313476565,
            "unit": "ns",
            "range": "± 573.1912510138853"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10341.223098754883,
            "unit": "ns",
            "range": "± 59.22378511657235"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 11343.184755765475,
            "unit": "ns",
            "range": "± 13.406084707731573"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 26019.96999359131,
            "unit": "ns",
            "range": "± 8.766928665124691"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 12246.113708496094,
            "unit": "ns",
            "range": "± 65.82350178486921"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 13740.145515441895,
            "unit": "ns",
            "range": "± 62.498252873815204"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 11996.154313307543,
            "unit": "ns",
            "range": "± 32.719683052392355"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 11350.967853800455,
            "unit": "ns",
            "range": "± 37.617725610499086"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 14271.868816891232,
            "unit": "ns",
            "range": "± 474.2524792289262"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 13877.499342854817,
            "unit": "ns",
            "range": "± 66.75433531556202"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 12772.367529062125,
            "unit": "ns",
            "range": "± 33.23743150737368"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 14040.628432053785,
            "unit": "ns",
            "range": "± 9.290154173743087"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 14197.596400960287,
            "unit": "ns",
            "range": "± 55.69446192657523"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 12565.809628295898,
            "unit": "ns",
            "range": "± 49.4273621210575"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 13315.771637980144,
            "unit": "ns",
            "range": "± 61.60198773215763"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 130892.6236328125,
            "unit": "ns",
            "range": "± 1549.685691714905"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 140226.36886160713,
            "unit": "ns",
            "range": "± 844.597109065613"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 46983.90192464193,
            "unit": "ns",
            "range": "± 247.00660406926275"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 53415.74555751256,
            "unit": "ns",
            "range": "± 187.24766141712274"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 247875.6283482143,
            "unit": "ns",
            "range": "± 635.797712790076"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 55719.053458658855,
            "unit": "ns",
            "range": "± 238.33809131014726"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 58533.96487630208,
            "unit": "ns",
            "range": "± 155.7949112395365"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 61882.480407714844,
            "unit": "ns",
            "range": "± 65.19296594880063"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 64546.09648640951,
            "unit": "ns",
            "range": "± 36.654768061948886"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 159778.06284877233,
            "unit": "ns",
            "range": "± 1163.3545059339442"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 235257.9204752604,
            "unit": "ns",
            "range": "± 1576.1163075919267"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 151122.01798502603,
            "unit": "ns",
            "range": "± 1012.1679757342571"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 221541.84073893228,
            "unit": "ns",
            "range": "± 1053.6049946744026"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 154364.29541015625,
            "unit": "ns",
            "range": "± 902.7408796261403"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 156397.89396972657,
            "unit": "ns",
            "range": "± 2249.065768239311"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 223776.86407877604,
            "unit": "ns",
            "range": "± 1221.8125373307896"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 123324.4734061105,
            "unit": "ns",
            "range": "± 1023.4566021821134"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 133050.5305094401,
            "unit": "ns",
            "range": "± 1376.0870204245696"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 45216.589698204625,
            "unit": "ns",
            "range": "± 65.6737882547862"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 54096.18019816081,
            "unit": "ns",
            "range": "± 228.37294492725454"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 239853.05313546318,
            "unit": "ns",
            "range": "± 477.3619208900994"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 55586.78974797176,
            "unit": "ns",
            "range": "± 59.460105620381434"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 59067.484883626305,
            "unit": "ns",
            "range": "± 140.21069733558497"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 60410.023340861,
            "unit": "ns",
            "range": "± 44.49249885290491"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 66660.59140450614,
            "unit": "ns",
            "range": "± 116.98212735965079"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 148306.49806315106,
            "unit": "ns",
            "range": "± 704.9230429848209"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 190370.3158656529,
            "unit": "ns",
            "range": "± 495.23999125909364"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 142740.89521135602,
            "unit": "ns",
            "range": "± 253.75892882215456"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 184487.73037109376,
            "unit": "ns",
            "range": "± 868.4504908978176"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 143972.69119466146,
            "unit": "ns",
            "range": "± 519.5440392586"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 143345.74936523437,
            "unit": "ns",
            "range": "± 502.19888652251046"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 188521.31127929688,
            "unit": "ns",
            "range": "± 928.4938341538789"
          }
        ]
      }
    ],
    "Operations.SetOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744676246762,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 158755.74923270088,
            "unit": "ns",
            "range": "± 1285.3387375644602"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 80806.0889444987,
            "unit": "ns",
            "range": "± 783.9353649668195"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10271.215845743814,
            "unit": "ns",
            "range": "± 60.888003099534956"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 11753.696689860026,
            "unit": "ns",
            "range": "± 53.946259532195796"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 25820.710622934195,
            "unit": "ns",
            "range": "± 46.166970143184955"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 12260.098086547852,
            "unit": "ns",
            "range": "± 86.874995365841"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 13868.463970947265,
            "unit": "ns",
            "range": "± 47.8929255717475"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 12892.647900390624,
            "unit": "ns",
            "range": "± 65.7016907787552"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 11440.483924357097,
            "unit": "ns",
            "range": "± 57.76572792838625"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 12728.96109008789,
            "unit": "ns",
            "range": "± 67.0190632027918"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 14093.59129638672,
            "unit": "ns",
            "range": "± 62.69924561532709"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 12941.771493530274,
            "unit": "ns",
            "range": "± 69.74224513303673"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 14320.44857381185,
            "unit": "ns",
            "range": "± 48.36280030312956"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 14305.436737060547,
            "unit": "ns",
            "range": "± 49.18630508493231"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 12560.168207804361,
            "unit": "ns",
            "range": "± 46.61877442811899"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 13525.81012071882,
            "unit": "ns",
            "range": "± 48.17603331863818"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 173874.43184988838,
            "unit": "ns",
            "range": "± 1485.4425426232715"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 186232.9086263021,
            "unit": "ns",
            "range": "± 1046.3141818423412"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 53127.97057291667,
            "unit": "ns",
            "range": "± 272.77801534280644"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 49853.90385219029,
            "unit": "ns",
            "range": "± 177.49329814746486"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 251860.37894112724,
            "unit": "ns",
            "range": "± 1138.3655350638714"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 50789.41637369792,
            "unit": "ns",
            "range": "± 184.89026692805"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 56846.27803751628,
            "unit": "ns",
            "range": "± 358.7490570114353"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 62599.83307698568,
            "unit": "ns",
            "range": "± 238.9040245852202"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 73483.45511067708,
            "unit": "ns",
            "range": "± 444.3731371683706"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 157666.33446451824,
            "unit": "ns",
            "range": "± 1124.4910485341618"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 251887.6392299107,
            "unit": "ns",
            "range": "± 1204.587198019202"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 151362.49778645832,
            "unit": "ns",
            "range": "± 669.0777196490825"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 241096.48372395834,
            "unit": "ns",
            "range": "± 1019.8938926940417"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 162590.55765206474,
            "unit": "ns",
            "range": "± 522.0608804789233"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 159409.36088679387,
            "unit": "ns",
            "range": "± 559.6265998981714"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 255455.77955729168,
            "unit": "ns",
            "range": "± 1745.7786972222416"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 173843.42529296875,
            "unit": "ns",
            "range": "± 1809.5688547686714"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 177129.75756835938,
            "unit": "ns",
            "range": "± 824.2041841886327"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 53228.23083496094,
            "unit": "ns",
            "range": "± 148.71403286925215"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 47429.63657052176,
            "unit": "ns",
            "range": "± 175.77693148706942"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 245813.00617327009,
            "unit": "ns",
            "range": "± 1178.351357745247"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 48333.66243896484,
            "unit": "ns",
            "range": "± 190.3238243439462"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 58498.1622140067,
            "unit": "ns",
            "range": "± 161.322290431368"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 55243.37814534505,
            "unit": "ns",
            "range": "± 102.89189124638568"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 63952.138881138395,
            "unit": "ns",
            "range": "± 279.2583964674512"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 154241.30067545574,
            "unit": "ns",
            "range": "± 1024.5069920589906"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 217377.0030843099,
            "unit": "ns",
            "range": "± 1015.8557107727372"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 143865.06537737165,
            "unit": "ns",
            "range": "± 449.26951326161645"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 203396.8908610026,
            "unit": "ns",
            "range": "± 651.879859783768"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 155585.7741373698,
            "unit": "ns",
            "range": "± 671.9750765578239"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 150122.84563802084,
            "unit": "ns",
            "range": "± 536.2876485108382"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 203573.9790690104,
            "unit": "ns",
            "range": "± 768.660312396739"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770634630,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 160941.94114583332,
            "unit": "ns",
            "range": "± 1472.1988213678508"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 79750.89369419643,
            "unit": "ns",
            "range": "± 472.5397425474119"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10184.461994425455,
            "unit": "ns",
            "range": "± 96.79640447814022"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 11681.703912099203,
            "unit": "ns",
            "range": "± 14.637682663297758"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 28327.12838236491,
            "unit": "ns",
            "range": "± 69.4076142798042"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 12328.870005289713,
            "unit": "ns",
            "range": "± 70.46312164971755"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 14020.502024332682,
            "unit": "ns",
            "range": "± 58.40262108146273"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 12908.053388323102,
            "unit": "ns",
            "range": "± 97.73950996041948"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 11228.298953247071,
            "unit": "ns",
            "range": "± 97.80214628313253"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 12570.348690795898,
            "unit": "ns",
            "range": "± 42.404547212477304"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 14097.176206461589,
            "unit": "ns",
            "range": "± 71.58951571183614"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 12852.591481526693,
            "unit": "ns",
            "range": "± 83.01672755037117"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 14914.55632832845,
            "unit": "ns",
            "range": "± 86.44665411742871"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 15354.989452655498,
            "unit": "ns",
            "range": "± 59.47085416091163"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 12528.896264212472,
            "unit": "ns",
            "range": "± 6.463454415866862"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 13604.333868844169,
            "unit": "ns",
            "range": "± 39.89425947872057"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 171178.3563232422,
            "unit": "ns",
            "range": "± 887.0653911529472"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 183671.7466308594,
            "unit": "ns",
            "range": "± 923.2874732687039"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 52678.542669677736,
            "unit": "ns",
            "range": "± 207.279045639968"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 49720.189806256974,
            "unit": "ns",
            "range": "± 282.00679977405827"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 246239.94193209134,
            "unit": "ns",
            "range": "± 563.4461329000471"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 50375.54998544546,
            "unit": "ns",
            "range": "± 183.12204188037413"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 56884.514164225264,
            "unit": "ns",
            "range": "± 244.9402856913028"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 56576.64135306222,
            "unit": "ns",
            "range": "± 131.26776185883983"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 72738.70880533854,
            "unit": "ns",
            "range": "± 355.679574775249"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 175057.35498046875,
            "unit": "ns",
            "range": "± 1209.6708664558985"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 255257.50458984374,
            "unit": "ns",
            "range": "± 1090.3855690841106"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 162980.90377604167,
            "unit": "ns",
            "range": "± 724.9076724861145"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 241105.155796596,
            "unit": "ns",
            "range": "± 1341.5410116600679"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 151891.26818847656,
            "unit": "ns",
            "range": "± 565.6763408497123"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 164841.44633789064,
            "unit": "ns",
            "range": "± 1131.494433618509"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 247481.3617466518,
            "unit": "ns",
            "range": "± 1026.3302128040798"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 162835.0166015625,
            "unit": "ns",
            "range": "± 1550.550930938063"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 170439.59995117187,
            "unit": "ns",
            "range": "± 1255.2645905145846"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 52926.66924176897,
            "unit": "ns",
            "range": "± 140.69103310795492"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 48772.55711263021,
            "unit": "ns",
            "range": "± 261.69597017454737"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 233618.91435546876,
            "unit": "ns",
            "range": "± 891.1152992872236"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 51474.31984863281,
            "unit": "ns",
            "range": "± 167.0426939052057"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 58183.068044026695,
            "unit": "ns",
            "range": "± 205.66592890283118"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 56561.982130940756,
            "unit": "ns",
            "range": "± 156.93308710071668"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 61577.47919108073,
            "unit": "ns",
            "range": "± 368.52824145740675"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 153595.6821126302,
            "unit": "ns",
            "range": "± 725.4329339690007"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 206576.69838460287,
            "unit": "ns",
            "range": "± 680.9074698370648"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 154257.61796875,
            "unit": "ns",
            "range": "± 605.8334529522311"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 194932.83587239584,
            "unit": "ns",
            "range": "± 485.6062613559202"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 144946.2614095052,
            "unit": "ns",
            "range": "± 395.66653058801995"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 150837.79050990514,
            "unit": "ns",
            "range": "± 630.2872687903673"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 211862.98639322916,
            "unit": "ns",
            "range": "± 759.2279064293045"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744676262653,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 91972.8759765625,
            "unit": "ns",
            "range": "± 252.05020525475834"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 25292.97616141183,
            "unit": "ns",
            "range": "± 14.698288563610475"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 23469.388427734375,
            "unit": "ns",
            "range": "± 46.67739416760528"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 76400.27465820312,
            "unit": "ns",
            "range": "± 102.08957719521749"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 31497.749430338543,
            "unit": "ns",
            "range": "± 77.29387183523701"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 77279.99348958333,
            "unit": "ns",
            "range": "± 172.16660171401702"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 5556447.265625,
            "unit": "ns",
            "range": "± 132655.7171036883"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 155950.84033203125,
            "unit": "ns",
            "range": "± 13329.73791214589"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 91177.14059012277,
            "unit": "ns",
            "range": "± 293.42713667395185"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 25015.5764066256,
            "unit": "ns",
            "range": "± 7.063460795359138"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 23311.610848563058,
            "unit": "ns",
            "range": "± 30.906871031664426"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 77700.10986328125,
            "unit": "ns",
            "range": "± 89.44082217174267"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 30686.56968336839,
            "unit": "ns",
            "range": "± 55.48817203116321"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 76297.16959635417,
            "unit": "ns",
            "range": "± 182.286462961259"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 5608222.321428572,
            "unit": "ns",
            "range": "± 82629.32536184609"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 159655.43408203125,
            "unit": "ns",
            "range": "± 14635.575642559446"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 90488.66811899039,
            "unit": "ns",
            "range": "± 181.58074512340212"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 24729.64336688702,
            "unit": "ns",
            "range": "± 22.86121802971557"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 23311.707305908203,
            "unit": "ns",
            "range": "± 8.006035331064558"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 77120.13590494792,
            "unit": "ns",
            "range": "± 159.07089660549138"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 31960.98850795201,
            "unit": "ns",
            "range": "± 48.34360529571882"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 76652.01154436384,
            "unit": "ns",
            "range": "± 241.32090293573026"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 4559793.810096154,
            "unit": "ns",
            "range": "± 12564.101187010749"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 147439.21770368304,
            "unit": "ns",
            "range": "± 220.0764253572406"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 90324.79684012277,
            "unit": "ns",
            "range": "± 228.1487649922242"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 25350.4584757487,
            "unit": "ns",
            "range": "± 10.916744609233072"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 23617.185538155692,
            "unit": "ns",
            "range": "± 12.594473342717087"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 76989.75219726562,
            "unit": "ns",
            "range": "± 199.98867317768193"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 32041.67654854911,
            "unit": "ns",
            "range": "± 70.16582952235778"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 78653.96554129464,
            "unit": "ns",
            "range": "± 92.70615552423467"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 5094903.645833333,
            "unit": "ns",
            "range": "± 9154.998425730622"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 160687.24446614584,
            "unit": "ns",
            "range": "± 405.4267180966689"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 90706.97108677456,
            "unit": "ns",
            "range": "± 166.08300350979619"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 25296.844700404577,
            "unit": "ns",
            "range": "± 19.642206849549332"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 23330.16597202846,
            "unit": "ns",
            "range": "± 20.14852961931002"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 78458.95060221355,
            "unit": "ns",
            "range": "± 118.45502522309887"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 32722.039358956474,
            "unit": "ns",
            "range": "± 50.234851973186586"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 76741.74281529018,
            "unit": "ns",
            "range": "± 123.7620011420517"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 5113160.366586538,
            "unit": "ns",
            "range": "± 14239.886787742622"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 159777.6407877604,
            "unit": "ns",
            "range": "± 234.10869067434413"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770642677,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 89432.47767857143,
            "unit": "ns",
            "range": "± 205.60558767255765"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 25269.300624302454,
            "unit": "ns",
            "range": "± 19.228817455104682"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 23798.5600062779,
            "unit": "ns",
            "range": "± 18.138647633686464"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 76146.06846400669,
            "unit": "ns",
            "range": "± 271.3549016619768"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 31483.356323242188,
            "unit": "ns",
            "range": "± 91.09460834139912"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 77402.93863932292,
            "unit": "ns",
            "range": "± 170.85856982180192"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 5558926.7578125,
            "unit": "ns",
            "range": "± 124906.78462753988"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 155094.62182617188,
            "unit": "ns",
            "range": "± 13254.358830781624"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 90562.99250676081,
            "unit": "ns",
            "range": "± 246.0154969997601"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 25445.11906550481,
            "unit": "ns",
            "range": "± 11.954956183126972"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 23310.283987862724,
            "unit": "ns",
            "range": "± 10.780466781612063"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 77304.91755558894,
            "unit": "ns",
            "range": "± 248.781559520319"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 30318.693106515067,
            "unit": "ns",
            "range": "± 44.60972698025309"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 74557.6189313616,
            "unit": "ns",
            "range": "± 180.9558384176545"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 5554058.314732143,
            "unit": "ns",
            "range": "± 93818.53162866327"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 156460.48266601562,
            "unit": "ns",
            "range": "± 13631.039690223708"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 89892.29407677284,
            "unit": "ns",
            "range": "± 173.8074907031336"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 24647.794669015067,
            "unit": "ns",
            "range": "± 18.826619730098535"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 23498.670305524553,
            "unit": "ns",
            "range": "± 30.613819969455427"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 74959.70720563616,
            "unit": "ns",
            "range": "± 73.44054266977037"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 31819.639485677082,
            "unit": "ns",
            "range": "± 41.845911916466356"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 75812.89571126302,
            "unit": "ns",
            "range": "± 65.91024343687833"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 4535349.308894231,
            "unit": "ns",
            "range": "± 10063.590721460805"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 140928.5652669271,
            "unit": "ns",
            "range": "± 148.08874156320292"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 89691.6621907552,
            "unit": "ns",
            "range": "± 271.82668458089717"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 25336.9139451247,
            "unit": "ns",
            "range": "± 10.766894985662978"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 23504.964338030135,
            "unit": "ns",
            "range": "± 10.055071744645076"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 76215.24483816964,
            "unit": "ns",
            "range": "± 68.81688170239718"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 32109.261648995536,
            "unit": "ns",
            "range": "± 40.59866283664959"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 82045.37516276042,
            "unit": "ns",
            "range": "± 88.62493013745198"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 5087252.373798077,
            "unit": "ns",
            "range": "± 12757.943495510472"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 159745.3369140625,
            "unit": "ns",
            "range": "± 125.33888436078003"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 90385.49886067708,
            "unit": "ns",
            "range": "± 405.26252165826713"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 24954.102579752605,
            "unit": "ns",
            "range": "± 12.02970108336911"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 23528.61297607422,
            "unit": "ns",
            "range": "± 16.395936200688006"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 75833.1766764323,
            "unit": "ns",
            "range": "± 92.44033586205872"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 32145.428873697918,
            "unit": "ns",
            "range": "± 31.70859375479966"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 78234.87141927083,
            "unit": "ns",
            "range": "± 83.20780096425682"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 5085821.744791667,
            "unit": "ns",
            "range": "± 9696.789985407526"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 161023.3097330729,
            "unit": "ns",
            "range": "± 1264.5908123832735"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744676285293,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 91496.92220052083,
            "unit": "ns",
            "range": "± 176.35174824569802"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 12244.150034586588,
            "unit": "ns",
            "range": "± 10.487679033089172"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 10893.17621866862,
            "unit": "ns",
            "range": "± 24.706974949405506"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10500.667748084436,
            "unit": "ns",
            "range": "± 13.252780936042077"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 14613.936204176684,
            "unit": "ns",
            "range": "± 11.456484364694976"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 15667.267499651227,
            "unit": "ns",
            "range": "± 11.160353019062775"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 14628.973439534506,
            "unit": "ns",
            "range": "± 58.65774385900598"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 9805.307476337139,
            "unit": "ns",
            "range": "± 6.233796595210058"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 13406.50774637858,
            "unit": "ns",
            "range": "± 9.420961982242138"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 12837.361555833082,
            "unit": "ns",
            "range": "± 12.103569287981285"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 14382.930864606586,
            "unit": "ns",
            "range": "± 11.705193269506198"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 4469.771626790364,
            "unit": "ns",
            "range": "± 7.709466450324832"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12470.210484095982,
            "unit": "ns",
            "range": "± 24.23100547147914"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 15454.481036846455,
            "unit": "ns",
            "range": "± 9.734411969842578"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 14430.330330984933,
            "unit": "ns",
            "range": "± 12.0726518281538"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 106376.45733173077,
            "unit": "ns",
            "range": "± 216.63480666585997"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 38527.55650111607,
            "unit": "ns",
            "range": "± 76.00153700042671"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 38184.24159458705,
            "unit": "ns",
            "range": "± 80.8520853837641"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 43260.86687360491,
            "unit": "ns",
            "range": "± 37.27340096555794"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 62237.225341796875,
            "unit": "ns",
            "range": "± 171.41277234752593"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 86680.44668344352,
            "unit": "ns",
            "range": "± 149.69980931096626"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 43414.80189732143,
            "unit": "ns",
            "range": "± 62.8473859460234"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 31569.639078776043,
            "unit": "ns",
            "range": "± 54.13204535752581"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 44630.94613211496,
            "unit": "ns",
            "range": "± 92.67718115257603"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 60964.48974609375,
            "unit": "ns",
            "range": "± 296.0256035410777"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 50519.52491173377,
            "unit": "ns",
            "range": "± 73.07729030968841"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 4494.112269083659,
            "unit": "ns",
            "range": "± 6.635339425494774"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 52854.86661470853,
            "unit": "ns",
            "range": "± 168.07193183360707"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 41348.46932547433,
            "unit": "ns",
            "range": "± 85.97128248043829"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 43676.4413016183,
            "unit": "ns",
            "range": "± 49.01999946724831"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 92136.93062918527,
            "unit": "ns",
            "range": "± 105.18650889333404"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 38101.30179268973,
            "unit": "ns",
            "range": "± 132.55793410220883"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 40087.41019112723,
            "unit": "ns",
            "range": "± 54.01258029800431"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 42332.36368815104,
            "unit": "ns",
            "range": "± 93.85202597745878"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 54608.55625697545,
            "unit": "ns",
            "range": "± 91.01322902647611"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 78996.54296875,
            "unit": "ns",
            "range": "± 147.37950794804624"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 42928.32876352163,
            "unit": "ns",
            "range": "± 36.32126144620486"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 33248.67902483259,
            "unit": "ns",
            "range": "± 58.599083032450174"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 42769.08976236979,
            "unit": "ns",
            "range": "± 80.67512059250987"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 52470.69051106771,
            "unit": "ns",
            "range": "± 201.69444455344083"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 50264.96765136719,
            "unit": "ns",
            "range": "± 87.49310499754505"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 4498.609161376953,
            "unit": "ns",
            "range": "± 9.10916563716537"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 47090.51005045573,
            "unit": "ns",
            "range": "± 50.84102951153995"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 43260.93729654948,
            "unit": "ns",
            "range": "± 55.2208214760465"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 42482.303728376115,
            "unit": "ns",
            "range": "± 65.65805966851251"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770684387,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 96239.580078125,
            "unit": "ns",
            "range": "± 166.65971366252737"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 12166.8701171875,
            "unit": "ns",
            "range": "± 25.450857717652603"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 11149.20924260066,
            "unit": "ns",
            "range": "± 8.480075399829621"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10441.075369027945,
            "unit": "ns",
            "range": "± 6.855508707245515"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 14929.737745012555,
            "unit": "ns",
            "range": "± 15.77651585064932"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 15488.130798339844,
            "unit": "ns",
            "range": "± 9.771979730310536"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 13374.34545663687,
            "unit": "ns",
            "range": "± 9.266012043526135"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 9790.107218424479,
            "unit": "ns",
            "range": "± 11.011584476971752"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 13135.61037503756,
            "unit": "ns",
            "range": "± 20.56308925647468"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 12889.608177771936,
            "unit": "ns",
            "range": "± 12.918826696457474"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 14116.764538104717,
            "unit": "ns",
            "range": "± 13.313590430045197"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 4460.504313877651,
            "unit": "ns",
            "range": "± 7.675484406762524"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 12231.654005784254,
            "unit": "ns",
            "range": "± 13.622302802903937"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 15736.293683733258,
            "unit": "ns",
            "range": "± 9.871565544219433"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 14395.68568638393,
            "unit": "ns",
            "range": "± 13.4098421476022"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 107152.10745675223,
            "unit": "ns",
            "range": "± 239.17447717068018"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 37201.26507098858,
            "unit": "ns",
            "range": "± 51.53596839390671"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 40204.75628192608,
            "unit": "ns",
            "range": "± 46.61098783498208"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 41528.394368489586,
            "unit": "ns",
            "range": "± 50.757510057719486"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 66051.12386067708,
            "unit": "ns",
            "range": "± 229.4589273516451"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 89383.4883626302,
            "unit": "ns",
            "range": "± 287.0155234413722"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 43015.16854422433,
            "unit": "ns",
            "range": "± 44.69533319063427"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 30888.028971354168,
            "unit": "ns",
            "range": "± 46.922473949879006"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 43926.669921875,
            "unit": "ns",
            "range": "± 97.46070490475434"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 61734.766438802086,
            "unit": "ns",
            "range": "± 199.47607968806622"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 50844.81160481771,
            "unit": "ns",
            "range": "± 64.35844499955566"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 4579.930986676897,
            "unit": "ns",
            "range": "± 6.004427454190761"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 52048.38350736178,
            "unit": "ns",
            "range": "± 101.37876849473727"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 42011.68430873326,
            "unit": "ns",
            "range": "± 55.03365056911655"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 43701.03169759115,
            "unit": "ns",
            "range": "± 39.41560616824075"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 93916.64951869419,
            "unit": "ns",
            "range": "± 133.74252184228266"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 38851.27301897322,
            "unit": "ns",
            "range": "± 55.85761677775855"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 38737.55536760603,
            "unit": "ns",
            "range": "± 75.98410709851593"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 42975.32828194754,
            "unit": "ns",
            "range": "± 45.19659362187052"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 56260.27047293527,
            "unit": "ns",
            "range": "± 54.418362562790854"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 79609.0364896334,
            "unit": "ns",
            "range": "± 102.6134446616616"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 42160.38920084635,
            "unit": "ns",
            "range": "± 72.39573143157635"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 34617.626953125,
            "unit": "ns",
            "range": "± 49.39095906399677"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 43582.22900390625,
            "unit": "ns",
            "range": "± 56.60611566182776"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 55009.89481608073,
            "unit": "ns",
            "range": "± 102.76561216752573"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 52284.94614821214,
            "unit": "ns",
            "range": "± 75.21864456664507"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 4463.507243565151,
            "unit": "ns",
            "range": "± 7.711841716158239"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 47520.353044782365,
            "unit": "ns",
            "range": "± 62.1896426082409"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 42991.15687779018,
            "unit": "ns",
            "range": "± 48.64879529700714"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 43638.1787109375,
            "unit": "ns",
            "range": "± 59.69661349728687"
          }
        ]
      }
    ],
    "Operations.ScriptOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744676300970,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 96922.9024251302,
            "unit": "ns",
            "range": "± 188.65154321911314"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 26117.44646344866,
            "unit": "ns",
            "range": "± 45.84613473221286"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 23451.075744628906,
            "unit": "ns",
            "range": "± 28.922009403948252"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 76965.20385742188,
            "unit": "ns",
            "range": "± 157.74995931191543"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 33423.79811604818,
            "unit": "ns",
            "range": "± 56.47025890289585"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 77455.1775251116,
            "unit": "ns",
            "range": "± 93.55126846620088"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 5550308.035714285,
            "unit": "ns",
            "range": "± 96762.64428635334"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 153370.82421875,
            "unit": "ns",
            "range": "± 13161.568330162192"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 91371.64568219866,
            "unit": "ns",
            "range": "± 338.8157531031313"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 26627.61006673177,
            "unit": "ns",
            "range": "± 56.20515606678403"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 23547.981516520184,
            "unit": "ns",
            "range": "± 14.739033862884916"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 78148.75139508929,
            "unit": "ns",
            "range": "± 79.61543154656887"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 33118.56035505022,
            "unit": "ns",
            "range": "± 34.62679994097998"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 78259.30551382211,
            "unit": "ns",
            "range": "± 79.06324382863926"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 5643768.1640625,
            "unit": "ns",
            "range": "± 122966.70265441958"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 157519.12622070312,
            "unit": "ns",
            "range": "± 13260.918372706357"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 91307.80436197917,
            "unit": "ns",
            "range": "± 262.86182139154994"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 25525.367082868303,
            "unit": "ns",
            "range": "± 22.820338506533208"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 23914.932686941964,
            "unit": "ns",
            "range": "± 27.97446019515636"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 79409.64120718148,
            "unit": "ns",
            "range": "± 186.95256421532153"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 31908.478190104168,
            "unit": "ns",
            "range": "± 36.499306920836005"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 75657.91625976562,
            "unit": "ns",
            "range": "± 107.33049467401717"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 4617301.171875,
            "unit": "ns",
            "range": "± 16870.722188019405"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 144952.56723257212,
            "unit": "ns",
            "range": "± 199.13655594954668"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 91599.15597098214,
            "unit": "ns",
            "range": "± 247.41527487914038"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 25586.527663010816,
            "unit": "ns",
            "range": "± 19.220186191264705"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 23485.859985351562,
            "unit": "ns",
            "range": "± 21.7332781414228"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 77911.95819561298,
            "unit": "ns",
            "range": "± 143.46154013386524"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 31387.538655598957,
            "unit": "ns",
            "range": "± 51.2648537852778"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 76145.12369791667,
            "unit": "ns",
            "range": "± 140.42527266445452"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 5182497.330729167,
            "unit": "ns",
            "range": "± 7308.011423632533"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 161829.64913504463,
            "unit": "ns",
            "range": "± 176.06301408824407"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 90530.50255408653,
            "unit": "ns",
            "range": "± 365.0613850814078"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 25499.830627441406,
            "unit": "ns",
            "range": "± 16.49018152363823"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 23581.300236628605,
            "unit": "ns",
            "range": "± 10.553730074343168"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 76554.43725585938,
            "unit": "ns",
            "range": "± 206.07542227772979"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 30760.09297688802,
            "unit": "ns",
            "range": "± 42.55231021405509"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 76452.40565708706,
            "unit": "ns",
            "range": "± 121.15328512863674"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 5152684.440104167,
            "unit": "ns",
            "range": "± 11691.616833970202"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 155688.19767878606,
            "unit": "ns",
            "range": "± 244.72622100602038"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770645443,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,Limit)",
            "value": 92114.82950846355,
            "unit": "ns",
            "range": "± 627.805825406418"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,Limit)",
            "value": 26206.107788085938,
            "unit": "ns",
            "range": "± 134.13060930270325"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,Limit)",
            "value": 23626.393330891926,
            "unit": "ns",
            "range": "± 76.63311931443556"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,Limit)",
            "value": 79330.04516601562,
            "unit": "ns",
            "range": "± 348.1685577766132"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,Limit)",
            "value": 33387.88804274339,
            "unit": "ns",
            "range": "± 162.6747141484408"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,Limit)",
            "value": 76924.12027994792,
            "unit": "ns",
            "range": "± 269.5281679544563"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,Limit)",
            "value": 5488600.240384615,
            "unit": "ns",
            "range": "± 77500.81627422676"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,Limit)",
            "value": 154327.11938476562,
            "unit": "ns",
            "range": "± 13924.659369684983"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Managed,None)",
            "value": 91672.98671177456,
            "unit": "ns",
            "range": "± 474.319807987984"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Managed,None)",
            "value": 26327.781575520832,
            "unit": "ns",
            "range": "± 66.67291185748248"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Managed,None)",
            "value": 23586.67485163762,
            "unit": "ns",
            "range": "± 56.93784993560314"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Managed,None)",
            "value": 77043.24776785714,
            "unit": "ns",
            "range": "± 241.8746579876735"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Managed,None)",
            "value": 33046.58203125,
            "unit": "ns",
            "range": "± 42.7607145738841"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Managed,None)",
            "value": 78383.3915201823,
            "unit": "ns",
            "range": "± 603.6901768730492"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Managed,None)",
            "value": 5651466.294642857,
            "unit": "ns",
            "range": "± 87417.78533615655"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Managed,None)",
            "value": 155706.78076171875,
            "unit": "ns",
            "range": "± 13757.391902013764"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Native,None)",
            "value": 90922.18191964286,
            "unit": "ns",
            "range": "± 390.69656702008325"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Native,None)",
            "value": 25718.82607596261,
            "unit": "ns",
            "range": "± 95.65851629715061"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Native,None)",
            "value": 23824.21641031901,
            "unit": "ns",
            "range": "± 74.33100725289502"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Native,None)",
            "value": 77242.8047688802,
            "unit": "ns",
            "range": "± 518.7296204221468"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Native,None)",
            "value": 32438.72811453683,
            "unit": "ns",
            "range": "± 157.3044276620825"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Native,None)",
            "value": 75977.40844726562,
            "unit": "ns",
            "range": "± 591.2058420729983"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Native,None)",
            "value": 4596404.854910715,
            "unit": "ns",
            "range": "± 41375.341440776945"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Native,None)",
            "value": 144113.50911458334,
            "unit": "ns",
            "range": "± 1062.0555676925396"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,Limit)",
            "value": 91663.95525251116,
            "unit": "ns",
            "range": "± 454.2736715517777"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,Limit)",
            "value": 25850.010375976562,
            "unit": "ns",
            "range": "± 141.00533066292346"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,Limit)",
            "value": 23682.268880208332,
            "unit": "ns",
            "range": "± 113.87296001528068"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,Limit)",
            "value": 76531.08805338542,
            "unit": "ns",
            "range": "± 319.76564071715774"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,Limit)",
            "value": 30969.071306501115,
            "unit": "ns",
            "range": "± 176.45648840313848"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,Limit)",
            "value": 81450.61971028645,
            "unit": "ns",
            "range": "± 554.0387578799857"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,Limit)",
            "value": 5154270.042067308,
            "unit": "ns",
            "range": "± 30608.995327769226"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,Limit)",
            "value": 160503.13197544642,
            "unit": "ns",
            "range": "± 1527.204732448021"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptLoad(Params: Tracked,None)",
            "value": 92671.5352376302,
            "unit": "ns",
            "range": "± 712.8780092241625"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsTrue(Params: Tracked,None)",
            "value": 25722.4516805013,
            "unit": "ns",
            "range": "± 136.23852371912986"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ScriptExistsFalse(Params: Tracked,None)",
            "value": 23661.793721516926,
            "unit": "ns",
            "range": "± 98.35655485671266"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.Eval(Params: Tracked,None)",
            "value": 75420.33772786458,
            "unit": "ns",
            "range": "± 103.35446930345033"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.EvalSha(Params: Tracked,None)",
            "value": 31231.016235351562,
            "unit": "ns",
            "range": "± 158.7539118515723"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.SmallScript(Params: Tracked,None)",
            "value": 78755.4248046875,
            "unit": "ns",
            "range": "± 565.243845621241"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.LargeScript(Params: Tracked,None)",
            "value": 5048044.381009615,
            "unit": "ns",
            "range": "± 29876.512556547936"
          },
          {
            "name": "BDN.benchmark.Operations.ScriptOperations.ArrayReturn(Params: Tracked,None)",
            "value": 155011.66522686297,
            "unit": "ns",
            "range": "± 115.4587716597454"
          }
        ]
      }
    ],
    "Operations.HashObjectOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744676330927,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 104560.11108398438,
            "unit": "ns",
            "range": "± 251.13205339588157"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 12497.26051917443,
            "unit": "ns",
            "range": "± 18.974059628010107"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 12028.980102539062,
            "unit": "ns",
            "range": "± 15.971616731626142"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10578.394753592354,
            "unit": "ns",
            "range": "± 10.317336407847211"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 15041.543109600361,
            "unit": "ns",
            "range": "± 19.811751198682572"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 17045.87871844952,
            "unit": "ns",
            "range": "± 9.520542115707883"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 15389.386698404947,
            "unit": "ns",
            "range": "± 20.393744610928703"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 9932.043750469502,
            "unit": "ns",
            "range": "± 4.650424924464202"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 15120.951334635416,
            "unit": "ns",
            "range": "± 22.194687568120834"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13052.52663748605,
            "unit": "ns",
            "range": "± 8.84329017686669"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 16083.38857014974,
            "unit": "ns",
            "range": "± 23.251679128316226"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 9290.010598989633,
            "unit": "ns",
            "range": "± 15.185102048490656"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 13605.956217447916,
            "unit": "ns",
            "range": "± 19.176438726159258"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 16051.498718261719,
            "unit": "ns",
            "range": "± 15.707147280703289"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 15581.149057241586,
            "unit": "ns",
            "range": "± 15.77424636673762"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 128406.8505859375,
            "unit": "ns",
            "range": "± 334.4601769435354"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 44833.721923828125,
            "unit": "ns",
            "range": "± 74.88450236801856"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 43892.25769042969,
            "unit": "ns",
            "range": "± 105.00983428374307"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 47792.33140211839,
            "unit": "ns",
            "range": "± 74.46095271112291"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 74531.89412434895,
            "unit": "ns",
            "range": "± 378.34629412920833"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 98909.89420572917,
            "unit": "ns",
            "range": "± 249.67009723920282"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 48298.29500638522,
            "unit": "ns",
            "range": "± 56.36518310740935"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 38607.322340745195,
            "unit": "ns",
            "range": "± 61.103983175349086"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 48231.939697265625,
            "unit": "ns",
            "range": "± 108.1999904695962"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 68885.90175083706,
            "unit": "ns",
            "range": "± 356.0351225252616"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 57991.81867327009,
            "unit": "ns",
            "range": "± 61.15786890022794"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 9343.431650797525,
            "unit": "ns",
            "range": "± 17.478846675535706"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 58665.60856745793,
            "unit": "ns",
            "range": "± 126.64091697404211"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 49777.21883138021,
            "unit": "ns",
            "range": "± 124.62148812221444"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 48070.005580357145,
            "unit": "ns",
            "range": "± 43.75181105479311"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 114298.74703543527,
            "unit": "ns",
            "range": "± 132.08451961909634"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 47381.59484863281,
            "unit": "ns",
            "range": "± 86.85408158931516"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 43339.35945951022,
            "unit": "ns",
            "range": "± 61.519661027875"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 47302.52802922176,
            "unit": "ns",
            "range": "± 56.76039946829833"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 63516.12112862723,
            "unit": "ns",
            "range": "± 230.4152751122267"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 91322.03194754464,
            "unit": "ns",
            "range": "± 133.6539767109971"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 46930.65490722656,
            "unit": "ns",
            "range": "± 69.39668148850197"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 38602.866908482145,
            "unit": "ns",
            "range": "± 72.77703788168525"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 48985.50537109375,
            "unit": "ns",
            "range": "± 151.28327576471978"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 69409.10034179688,
            "unit": "ns",
            "range": "± 78.14609942030312"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 56287.019856770836,
            "unit": "ns",
            "range": "± 81.00975303409847"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 9168.198067801339,
            "unit": "ns",
            "range": "± 17.92987764188986"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 52693.106951032365,
            "unit": "ns",
            "range": "± 84.44014512985336"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 47081.13446916853,
            "unit": "ns",
            "range": "± 141.49755122939285"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 48719.02791341146,
            "unit": "ns",
            "range": "± 105.98497342171001"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770686942,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: ACL)",
            "value": 107343.25764973958,
            "unit": "ns",
            "range": "± 833.8991960495903"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: ACL)",
            "value": 12798.491414388021,
            "unit": "ns",
            "range": "± 120.36019588589352"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: ACL)",
            "value": 12213.509165445963,
            "unit": "ns",
            "range": "± 80.64011778460542"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: ACL)",
            "value": 10684.891568697416,
            "unit": "ns",
            "range": "± 96.91724857907013"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: ACL)",
            "value": 15151.874542236328,
            "unit": "ns",
            "range": "± 65.05919610385351"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: ACL)",
            "value": 17566.488138834637,
            "unit": "ns",
            "range": "± 80.92542574582122"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: ACL)",
            "value": 14760.313975016275,
            "unit": "ns",
            "range": "± 75.66924463843868"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: ACL)",
            "value": 10099.613647460938,
            "unit": "ns",
            "range": "± 100.60507846556035"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: ACL)",
            "value": 15174.782206217447,
            "unit": "ns",
            "range": "± 85.0352324415696"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: ACL)",
            "value": 13193.19585164388,
            "unit": "ns",
            "range": "± 103.38707194650274"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: ACL)",
            "value": 14850.688171386719,
            "unit": "ns",
            "range": "± 49.843020186364285"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: ACL)",
            "value": 9358.998565673828,
            "unit": "ns",
            "range": "± 62.82275920982458"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: ACL)",
            "value": 13852.926526750836,
            "unit": "ns",
            "range": "± 86.73563869611412"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: ACL)",
            "value": 15749.70223563058,
            "unit": "ns",
            "range": "± 84.99086964700423"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: ACL)",
            "value": 15709.958292643229,
            "unit": "ns",
            "range": "± 137.76109334796828"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: AOF)",
            "value": 119471.31673177083,
            "unit": "ns",
            "range": "± 1050.2999170494113"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: AOF)",
            "value": 44727.65869140625,
            "unit": "ns",
            "range": "± 370.56243716162805"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: AOF)",
            "value": 42524.58028157552,
            "unit": "ns",
            "range": "± 395.0521430559967"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: AOF)",
            "value": 48979.801025390625,
            "unit": "ns",
            "range": "± 273.0939485476463"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: AOF)",
            "value": 70134.62768554688,
            "unit": "ns",
            "range": "± 446.1830241710661"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: AOF)",
            "value": 102086.04642427884,
            "unit": "ns",
            "range": "± 386.59110443047626"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: AOF)",
            "value": 47900.18798828125,
            "unit": "ns",
            "range": "± 301.05689456781704"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: AOF)",
            "value": 39501.25284830729,
            "unit": "ns",
            "range": "± 199.77518975766375"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: AOF)",
            "value": 49955.914713541664,
            "unit": "ns",
            "range": "± 415.23977160978785"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: AOF)",
            "value": 72146.38346354167,
            "unit": "ns",
            "range": "± 771.898008476701"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: AOF)",
            "value": 56146.051025390625,
            "unit": "ns",
            "range": "± 228.14396981835714"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: AOF)",
            "value": 9420.757446289062,
            "unit": "ns",
            "range": "± 69.26393593711883"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: AOF)",
            "value": 59356.446533203125,
            "unit": "ns",
            "range": "± 386.6783764173818"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: AOF)",
            "value": 53685.01979282924,
            "unit": "ns",
            "range": "± 197.51190768515522"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: AOF)",
            "value": 48701.20567908654,
            "unit": "ns",
            "range": "± 116.658683707086"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetDel(Params: None)",
            "value": 120013.53190104167,
            "unit": "ns",
            "range": "± 751.171694394377"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HExists(Params: None)",
            "value": 43893.02411760603,
            "unit": "ns",
            "range": "± 403.44744821186777"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGet(Params: None)",
            "value": 44017.31587727865,
            "unit": "ns",
            "range": "± 377.08860292873084"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HGetAll(Params: None)",
            "value": 47045.01953125,
            "unit": "ns",
            "range": "± 445.35386268070187"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrby(Params: None)",
            "value": 63474.680989583336,
            "unit": "ns",
            "range": "± 609.1412478765593"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HIncrbyFloat(Params: None)",
            "value": 92365.8220563616,
            "unit": "ns",
            "range": "± 532.8009399264735"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HKeys(Params: None)",
            "value": 48562.213541666664,
            "unit": "ns",
            "range": "± 529.2966093459172"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HLen(Params: None)",
            "value": 39032.09167480469,
            "unit": "ns",
            "range": "± 275.93049152608285"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMGet(Params: None)",
            "value": 51303.202311197914,
            "unit": "ns",
            "range": "± 156.37737328436"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HMSet(Params: None)",
            "value": 65328.03181966146,
            "unit": "ns",
            "range": "± 570.1130162947776"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HRandField(Params: None)",
            "value": 57735.22888183594,
            "unit": "ns",
            "range": "± 593.6946452875449"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HScan(Params: None)",
            "value": 9273.616438645582,
            "unit": "ns",
            "range": "± 31.684437997774165"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HSetNx(Params: None)",
            "value": 53911.5976969401,
            "unit": "ns",
            "range": "± 360.2998090307488"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HStrLen(Params: None)",
            "value": 47413.13456217448,
            "unit": "ns",
            "range": "± 154.5626232916966"
          },
          {
            "name": "BDN.benchmark.Operations.HashObjectOperations.HVals(Params: None)",
            "value": 48698.553466796875,
            "unit": "ns",
            "range": "± 293.969546976996"
          }
        ]
      }
    ],
    "Operations.SetOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744676444988,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 116594.4775390625,
            "unit": "ns",
            "range": "± 615.0954689908154"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 60103.675130208336,
            "unit": "ns",
            "range": "± 209.28888540044701"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10341.878291538784,
            "unit": "ns",
            "range": "± 10.227653176462336"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 14537.988891601562,
            "unit": "ns",
            "range": "± 45.418509407974845"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 29002.327669583836,
            "unit": "ns",
            "range": "± 26.00491233235589"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 15026.84347970145,
            "unit": "ns",
            "range": "± 11.13799811673749"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 18443.74019077846,
            "unit": "ns",
            "range": "± 89.16617500355719"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 16280.184584397535,
            "unit": "ns",
            "range": "± 17.185316684906784"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 12212.515476771763,
            "unit": "ns",
            "range": "± 13.33845383901473"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 14180.555674235025,
            "unit": "ns",
            "range": "± 19.361346245087567"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 19493.92120361328,
            "unit": "ns",
            "range": "± 36.80044491328564"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 15394.099426269531,
            "unit": "ns",
            "range": "± 8.780857866074042"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 19743.807329450334,
            "unit": "ns",
            "range": "± 29.76595348412797"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 19395.347290039062,
            "unit": "ns",
            "range": "± 28.565227949837922"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 15011.350359235492,
            "unit": "ns",
            "range": "± 24.841139975875016"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 16263.282423753004,
            "unit": "ns",
            "range": "± 36.77518278650507"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 121021.07014973958,
            "unit": "ns",
            "range": "± 816.245600335857"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 129054.29850260417,
            "unit": "ns",
            "range": "± 934.8084736720697"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 33478.519694010414,
            "unit": "ns",
            "range": "± 34.3500313966007"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 41255.280949519234,
            "unit": "ns",
            "range": "± 73.71541183840309"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 190671.20267427884,
            "unit": "ns",
            "range": "± 448.18407396631505"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 42942.982700892855,
            "unit": "ns",
            "range": "± 83.13364277639954"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 50524.1103108724,
            "unit": "ns",
            "range": "± 84.1750626836689"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 50097.39031110491,
            "unit": "ns",
            "range": "± 91.49490360118266"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 54381.27223423549,
            "unit": "ns",
            "range": "± 295.05564907184146"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 126049.33558872768,
            "unit": "ns",
            "range": "± 835.1185126824282"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 213676.03515625,
            "unit": "ns",
            "range": "± 934.8428822732403"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 131351.17710658483,
            "unit": "ns",
            "range": "± 633.2980453355966"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 187938.51725260416,
            "unit": "ns",
            "range": "± 708.530590883447"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 122529.052734375,
            "unit": "ns",
            "range": "± 930.6937021399997"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 117316.94864908855,
            "unit": "ns",
            "range": "± 375.7028053844475"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 185524.12806919642,
            "unit": "ns",
            "range": "± 570.5959359742598"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 115182.23031850961,
            "unit": "ns",
            "range": "± 470.8074147459185"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 126203.3944936899,
            "unit": "ns",
            "range": "± 416.56152737213534"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 33668.96769205729,
            "unit": "ns",
            "range": "± 44.35140279193034"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 41363.56180826823,
            "unit": "ns",
            "range": "± 78.10023405261863"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 173768.36181640625,
            "unit": "ns",
            "range": "± 294.718046432777"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 43068.41665414663,
            "unit": "ns",
            "range": "± 45.48723253046505"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 53592.00398763021,
            "unit": "ns",
            "range": "± 424.40943065873887"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 50412.7402750651,
            "unit": "ns",
            "range": "± 80.37568323634164"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 53643.489728655135,
            "unit": "ns",
            "range": "± 85.94384802224526"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 120960.5147298177,
            "unit": "ns",
            "range": "± 747.1869918058134"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 165275.44642857142,
            "unit": "ns",
            "range": "± 1383.4009712142615"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 114063.61519949777,
            "unit": "ns",
            "range": "± 212.58605863163052"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 153851.4369419643,
            "unit": "ns",
            "range": "± 675.3211618517191"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 113040.80374581473,
            "unit": "ns",
            "range": "± 280.29280256371493"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 111839.00495256696,
            "unit": "ns",
            "range": "± 330.66311106639"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 151750.2099609375,
            "unit": "ns",
            "range": "± 661.9071898537559"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770876525,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 114688.18708147321,
            "unit": "ns",
            "range": "± 460.6952456968052"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 58871.90072195871,
            "unit": "ns",
            "range": "± 103.0363790109333"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10339.934590657553,
            "unit": "ns",
            "range": "± 17.036539031863043"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 13612.377166748047,
            "unit": "ns",
            "range": "± 23.099229178676676"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 28261.5235548753,
            "unit": "ns",
            "range": "± 11.400137801356104"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 13854.548950195312,
            "unit": "ns",
            "range": "± 11.15867421416634"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 18565.800258091516,
            "unit": "ns",
            "range": "± 17.80258525986846"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 16920.770263671875,
            "unit": "ns",
            "range": "± 46.91272363512634"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 12171.518816266742,
            "unit": "ns",
            "range": "± 9.941887236304911"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 14205.282265799386,
            "unit": "ns",
            "range": "± 12.937248180231563"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 18657.45391845703,
            "unit": "ns",
            "range": "± 17.516704464090424"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 15395.023672921317,
            "unit": "ns",
            "range": "± 13.417764329252382"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 19547.445678710938,
            "unit": "ns",
            "range": "± 18.374060656864405"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 19602.99825032552,
            "unit": "ns",
            "range": "± 20.763966805163967"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 14806.804765973773,
            "unit": "ns",
            "range": "± 5.401874370520984"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 16340.290949894832,
            "unit": "ns",
            "range": "± 23.349757816115428"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 122109.65576171875,
            "unit": "ns",
            "range": "± 848.4461381728914"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 130618.96597055289,
            "unit": "ns",
            "range": "± 495.07345603073975"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 33729.55017089844,
            "unit": "ns",
            "range": "± 28.562182951332783"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 41557.046712239586,
            "unit": "ns",
            "range": "± 50.65767626038893"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 187059.72243088944,
            "unit": "ns",
            "range": "± 336.81523107695796"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 42748.34829477163,
            "unit": "ns",
            "range": "± 42.50911678693197"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 50120.14887883113,
            "unit": "ns",
            "range": "± 41.20777815454101"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 49983.09997558594,
            "unit": "ns",
            "range": "± 106.21847335119811"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 59786.99910481771,
            "unit": "ns",
            "range": "± 152.7489826697659"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 125368.6962890625,
            "unit": "ns",
            "range": "± 742.3616928931403"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 206136.4558919271,
            "unit": "ns",
            "range": "± 1442.1031567518498"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 114808.44988141741,
            "unit": "ns",
            "range": "± 409.3567946756908"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 200742.86733774038,
            "unit": "ns",
            "range": "± 651.1271703276926"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 123230.21240234375,
            "unit": "ns",
            "range": "± 1066.7028765744722"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 123621.39078776042,
            "unit": "ns",
            "range": "± 787.09335660631"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 183121.1515299479,
            "unit": "ns",
            "range": "± 1259.747668973436"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 115528.58642578125,
            "unit": "ns",
            "range": "± 659.312043241775"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 122637.14274088542,
            "unit": "ns",
            "range": "± 391.885511112924"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 32066.73104422433,
            "unit": "ns",
            "range": "± 39.605239619050565"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 41434.39287458147,
            "unit": "ns",
            "range": "± 45.49311796802868"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 172386.24441964287,
            "unit": "ns",
            "range": "± 321.5299842220921"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 42055.02950032552,
            "unit": "ns",
            "range": "± 62.31574093590808"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 52504.10461425781,
            "unit": "ns",
            "range": "± 59.27497164299331"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 51006.868896484375,
            "unit": "ns",
            "range": "± 141.6688166441513"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 53344.85735212053,
            "unit": "ns",
            "range": "± 78.04696747978164"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 117047.91447566106,
            "unit": "ns",
            "range": "± 212.22057038039313"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 161955.95528738838,
            "unit": "ns",
            "range": "± 1306.5207831371686"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 108342.37060546875,
            "unit": "ns",
            "range": "± 211.63268584129315"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 154458.80859375,
            "unit": "ns",
            "range": "± 1464.0066245877529"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 112348.90921456473,
            "unit": "ns",
            "range": "± 227.8216242483617"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 114272.30130709134,
            "unit": "ns",
            "range": "± 522.6282227337988"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 154194.15364583334,
            "unit": "ns",
            "range": "± 199.33636222764136"
          }
        ]
      }
    ],
    "Operations.SetOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744676497827,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 129107.04671223958,
            "unit": "ns",
            "range": "± 361.5234053344979"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 68869.81026785714,
            "unit": "ns",
            "range": "± 141.8143549087874"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10464.742102989783,
            "unit": "ns",
            "range": "± 13.438062446108813"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 15169.332357553336,
            "unit": "ns",
            "range": "± 13.971773492930243"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 32351.54549734933,
            "unit": "ns",
            "range": "± 21.810713453871113"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 15784.244864327567,
            "unit": "ns",
            "range": "± 15.870234563790314"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 19798.399658203125,
            "unit": "ns",
            "range": "± 48.27705933769275"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 18145.845249720984,
            "unit": "ns",
            "range": "± 20.25702294817017"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 12318.474520169771,
            "unit": "ns",
            "range": "± 18.772470707009578"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 16201.678114670973,
            "unit": "ns",
            "range": "± 14.328075542189394"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 19846.203831263952,
            "unit": "ns",
            "range": "± 49.058827151995665"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 16465.369306291854,
            "unit": "ns",
            "range": "± 114.57562371515489"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 21528.23498065655,
            "unit": "ns",
            "range": "± 21.9212204323041"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 21829.994405110676,
            "unit": "ns",
            "range": "± 96.02550612928914"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 16905.977102426383,
            "unit": "ns",
            "range": "± 34.12218980782769"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 17692.901259202223,
            "unit": "ns",
            "range": "± 49.54513082254664"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 145005.41748046875,
            "unit": "ns",
            "range": "± 441.5641299576238"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 149639.73214285713,
            "unit": "ns",
            "range": "± 772.7705253763615"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 38664.30358886719,
            "unit": "ns",
            "range": "± 51.791521011000725"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 45697.205025809155,
            "unit": "ns",
            "range": "± 132.5584217396949"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 215797.7351888021,
            "unit": "ns",
            "range": "± 588.2295943332326"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 47723.21646554129,
            "unit": "ns",
            "range": "± 222.78049034199915"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 57093.878173828125,
            "unit": "ns",
            "range": "± 99.3303879027804"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 55131.803240094865,
            "unit": "ns",
            "range": "± 171.38495229039623"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 57130.53719656808,
            "unit": "ns",
            "range": "± 96.1090939606715"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 141045.50537109375,
            "unit": "ns",
            "range": "± 711.5427780290227"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 234386.0164388021,
            "unit": "ns",
            "range": "± 553.5304388158712"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 140563.52364676338,
            "unit": "ns",
            "range": "± 737.9635507103071"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 213163.55305989584,
            "unit": "ns",
            "range": "± 856.304830781818"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 137123.7508138021,
            "unit": "ns",
            "range": "± 666.2182985559932"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 143051.14990234375,
            "unit": "ns",
            "range": "± 1129.426766852095"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 217263.2283528646,
            "unit": "ns",
            "range": "± 375.6720651006076"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 141245.25495256696,
            "unit": "ns",
            "range": "± 337.4689566916486"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 147859.28548177084,
            "unit": "ns",
            "range": "± 521.5072385709423"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 38288.106830303484,
            "unit": "ns",
            "range": "± 33.13833286462679"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 47659.09627278646,
            "unit": "ns",
            "range": "± 93.25230948764629"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 220520.91761997767,
            "unit": "ns",
            "range": "± 346.660829858659"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 48767.5537109375,
            "unit": "ns",
            "range": "± 62.250968671958965"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 56549.53482491629,
            "unit": "ns",
            "range": "± 95.03925416993873"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 55891.34908040365,
            "unit": "ns",
            "range": "± 109.1018317241728"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 57776.44958496094,
            "unit": "ns",
            "range": "± 120.99533307331095"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 134477.69426618304,
            "unit": "ns",
            "range": "± 388.1624872200045"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 192287.20005580358,
            "unit": "ns",
            "range": "± 369.66745350494847"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 124002.02287946429,
            "unit": "ns",
            "range": "± 262.17103401164354"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 184508.14615885416,
            "unit": "ns",
            "range": "± 820.6606533108495"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 124170.25709885817,
            "unit": "ns",
            "range": "± 148.1678920706014"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 124748.78824869792,
            "unit": "ns",
            "range": "± 333.09380814787056"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 173208.07931082588,
            "unit": "ns",
            "range": "± 373.9898467312132"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744770824821,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: ACL)",
            "value": 140656.77020733172,
            "unit": "ns",
            "range": "± 602.1417105277327"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: ACL)",
            "value": 69774.66634114583,
            "unit": "ns",
            "range": "± 108.34161245146791"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: ACL)",
            "value": 10502.270634969076,
            "unit": "ns",
            "range": "± 11.53580026412903"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: ACL)",
            "value": 15110.508610652043,
            "unit": "ns",
            "range": "± 18.174018825783282"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: ACL)",
            "value": 31949.720052083332,
            "unit": "ns",
            "range": "± 32.60296256604306"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: ACL)",
            "value": 14900.071970621744,
            "unit": "ns",
            "range": "± 10.730133580459151"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: ACL)",
            "value": 19754.07206217448,
            "unit": "ns",
            "range": "± 27.572274078204273"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: ACL)",
            "value": 18268.72297014509,
            "unit": "ns",
            "range": "± 39.21062506625203"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: ACL)",
            "value": 12633.370844523111,
            "unit": "ns",
            "range": "± 6.998723513013367"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: ACL)",
            "value": 15340.953499930245,
            "unit": "ns",
            "range": "± 20.395423029854864"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: ACL)",
            "value": 20500.50974527995,
            "unit": "ns",
            "range": "± 26.75029767618162"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: ACL)",
            "value": 17222.767203194755,
            "unit": "ns",
            "range": "± 11.759539408831206"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: ACL)",
            "value": 20928.702654157365,
            "unit": "ns",
            "range": "± 16.747816664632357"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: ACL)",
            "value": 21001.33303128756,
            "unit": "ns",
            "range": "± 16.085152705424733"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: ACL)",
            "value": 16878.93807547433,
            "unit": "ns",
            "range": "± 18.230200780884694"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: ACL)",
            "value": 18366.768595377605,
            "unit": "ns",
            "range": "± 13.741016694243205"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: AOF)",
            "value": 142147.26388113838,
            "unit": "ns",
            "range": "± 1088.4746956019867"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: AOF)",
            "value": 151945.6103515625,
            "unit": "ns",
            "range": "± 1234.7668194977302"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: AOF)",
            "value": 37736.59383138021,
            "unit": "ns",
            "range": "± 60.45474808553387"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: AOF)",
            "value": 47398.51946149553,
            "unit": "ns",
            "range": "± 78.5082951055474"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: AOF)",
            "value": 227892.34994741587,
            "unit": "ns",
            "range": "± 360.082628588772"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: AOF)",
            "value": 47518.074689592635,
            "unit": "ns",
            "range": "± 97.50021678717599"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: AOF)",
            "value": 57251.23770577567,
            "unit": "ns",
            "range": "± 100.62428582926594"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: AOF)",
            "value": 61514.48927659255,
            "unit": "ns",
            "range": "± 89.22543159059992"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: AOF)",
            "value": 57304.35994466146,
            "unit": "ns",
            "range": "± 84.11879885693102"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: AOF)",
            "value": 152411.6121419271,
            "unit": "ns",
            "range": "± 855.9633357324884"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: AOF)",
            "value": 226820.46595982142,
            "unit": "ns",
            "range": "± 819.2142627509371"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: AOF)",
            "value": 136387.94759114584,
            "unit": "ns",
            "range": "± 961.6185004304915"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: AOF)",
            "value": 217513.62915039062,
            "unit": "ns",
            "range": "± 337.1436148204437"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: AOF)",
            "value": 136475.27669270834,
            "unit": "ns",
            "range": "± 962.2164309715358"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: AOF)",
            "value": 142887.65462239584,
            "unit": "ns",
            "range": "± 787.8468264252988"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: AOF)",
            "value": 218119.96547154017,
            "unit": "ns",
            "range": "± 508.08157067865346"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddRem(Params: None)",
            "value": 133337.41048177084,
            "unit": "ns",
            "range": "± 567.3722843005963"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SAddPopSingle(Params: None)",
            "value": 137426.2241908482,
            "unit": "ns",
            "range": "± 674.7918472916285"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SCard(Params: None)",
            "value": 39300.7821219308,
            "unit": "ns",
            "range": "± 65.14179026570557"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMembers(Params: None)",
            "value": 47963.21146647135,
            "unit": "ns",
            "range": "± 76.55635816288031"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMoveTwice(Params: None)",
            "value": 205601.12630208334,
            "unit": "ns",
            "range": "± 307.8542581255434"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SIsMember(Params: None)",
            "value": 48938.785661969865,
            "unit": "ns",
            "range": "± 104.91852142480008"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SMIsMember(Params: None)",
            "value": 57021.39892578125,
            "unit": "ns",
            "range": "± 167.07301733915406"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SRandMemberSingle(Params: None)",
            "value": 55606.8516031901,
            "unit": "ns",
            "range": "± 99.55147386620361"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SScan(Params: None)",
            "value": 60551.57755533854,
            "unit": "ns",
            "range": "± 262.8954169507984"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnion(Params: None)",
            "value": 131445.4744466146,
            "unit": "ns",
            "range": "± 292.0719159798835"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SUnionStore(Params: None)",
            "value": 186656.65457589287,
            "unit": "ns",
            "range": "± 327.90599199822225"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInter(Params: None)",
            "value": 123770.29935396634,
            "unit": "ns",
            "range": "± 409.0972109094716"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterStore(Params: None)",
            "value": 186483.5880533854,
            "unit": "ns",
            "range": "± 559.7763277620958"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SInterCard(Params: None)",
            "value": 129625.39672851562,
            "unit": "ns",
            "range": "± 172.71603393081864"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiff(Params: None)",
            "value": 126257.48116629464,
            "unit": "ns",
            "range": "± 458.9262346534514"
          },
          {
            "name": "BDN.benchmark.Operations.SetOperations.SDiffStore(Params: None)",
            "value": 175092.94607979912,
            "unit": "ns",
            "range": "± 372.53118968166973"
          }
        ]
      }
    ],
    "Operations.SortedSetOperations (ubuntu-latest  net9.0 Release)": [
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
        "date": 1744676885133,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 118132.50908954327,
            "unit": "ns",
            "range": "± 433.107831030448"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11265.500478108725,
            "unit": "ns",
            "range": "± 32.45679410262773"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 12001.329621451241,
            "unit": "ns",
            "range": "± 36.111443589627015"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 13203.266492716471,
            "unit": "ns",
            "range": "± 54.35891646248303"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 15127.64859313965,
            "unit": "ns",
            "range": "± 43.4665065562603"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 13288.975037638347,
            "unit": "ns",
            "range": "± 42.09460273993264"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 13730.030051167805,
            "unit": "ns",
            "range": "± 55.70716217990885"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 14891.627415248326,
            "unit": "ns",
            "range": "± 72.45334486351898"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 16500.096763102214,
            "unit": "ns",
            "range": "± 124.27618029369931"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 13339.109438069661,
            "unit": "ns",
            "range": "± 51.04217641495239"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 75984.56460806039,
            "unit": "ns",
            "range": "± 195.43795740683132"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15200.67823486328,
            "unit": "ns",
            "range": "± 35.807223871034985"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 68913.62416178385,
            "unit": "ns",
            "range": "± 366.93024321729206"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 72403.82790902945,
            "unit": "ns",
            "range": "± 153.53916220511516"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 6970.018190002442,
            "unit": "ns",
            "range": "± 39.88461873243669"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12395.420293535504,
            "unit": "ns",
            "range": "± 23.924271292104788"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 16078.215049235027,
            "unit": "ns",
            "range": "± 127.8592239458949"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 11306.11006634052,
            "unit": "ns",
            "range": "± 9.757944362426066"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 71029.12719726562,
            "unit": "ns",
            "range": "± 256.82522574713585"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 70955.1325824444,
            "unit": "ns",
            "range": "± 89.80722279054876"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 73484.88898518881,
            "unit": "ns",
            "range": "± 526.2976578007764"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 12060.065970865886,
            "unit": "ns",
            "range": "± 48.92095261203122"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 5267.067789077759,
            "unit": "ns",
            "range": "± 10.19140552178607"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 12007.33659769694,
            "unit": "ns",
            "range": "± 48.75497259136063"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 13355.902669270834,
            "unit": "ns",
            "range": "± 50.6875083605287"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 17181.800216674805,
            "unit": "ns",
            "range": "± 25.862757282704496"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 131216.72565104166,
            "unit": "ns",
            "range": "± 539.4422112603129"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 48799.0973022461,
            "unit": "ns",
            "range": "± 231.45032868019308"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 71266.12768554688,
            "unit": "ns",
            "range": "± 636.7922576928013"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 128605.01135253906,
            "unit": "ns",
            "range": "± 1115.8708493594331"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 167351.66739908853,
            "unit": "ns",
            "range": "± 684.2240201463595"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 89935.68912760417,
            "unit": "ns",
            "range": "± 428.2646188753635"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 134337.4033954327,
            "unit": "ns",
            "range": "± 664.3839053824106"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 137454.40354817707,
            "unit": "ns",
            "range": "± 1133.243406939488"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 218149.54745047432,
            "unit": "ns",
            "range": "± 903.5870297202433"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 86340.1005859375,
            "unit": "ns",
            "range": "± 393.05529222608504"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 246937.2658935547,
            "unit": "ns",
            "range": "± 5588.400911082269"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 65827.11106363933,
            "unit": "ns",
            "range": "± 92.4291665025737"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 173060.46346609932,
            "unit": "ns",
            "range": "± 1235.1219575482553"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 171031.89173177083,
            "unit": "ns",
            "range": "± 1750.128026592921"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 6988.15033976237,
            "unit": "ns",
            "range": "± 20.78151629677237"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 79409.95092773438,
            "unit": "ns",
            "range": "± 331.4098691469121"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 121086.65597098214,
            "unit": "ns",
            "range": "± 1411.3122469598177"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 64610.427114633414,
            "unit": "ns",
            "range": "± 136.7618257111838"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 228687.82196514422,
            "unit": "ns",
            "range": "± 5839.17134537909"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 201896.57568359375,
            "unit": "ns",
            "range": "± 1197.9399041472923"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 204036.26945800782,
            "unit": "ns",
            "range": "± 2344.2663012079247"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 65208.038966587614,
            "unit": "ns",
            "range": "± 269.861021762224"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 5297.744560828576,
            "unit": "ns",
            "range": "± 8.257302499633528"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 65096.56558837891,
            "unit": "ns",
            "range": "± 222.57227745325974"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 140397.7455705915,
            "unit": "ns",
            "range": "± 922.5154209559657"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 228935.46254069012,
            "unit": "ns",
            "range": "± 894.3847272508302"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 116234.50520833333,
            "unit": "ns",
            "range": "± 538.3136808182361"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 49484.130078125,
            "unit": "ns",
            "range": "± 103.92891990872074"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 68495.7995867048,
            "unit": "ns",
            "range": "± 195.4988900885875"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 119589.8343069894,
            "unit": "ns",
            "range": "± 438.0370828258121"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 156725.00192057292,
            "unit": "ns",
            "range": "± 457.90456594199253"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 79818.53820800781,
            "unit": "ns",
            "range": "± 430.76700063889194"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 130476.89491373698,
            "unit": "ns",
            "range": "± 635.4085063755263"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 127807.42281668527,
            "unit": "ns",
            "range": "± 422.74837527995004"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 181706.80016276042,
            "unit": "ns",
            "range": "± 743.2505346022892"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 88420.32096354167,
            "unit": "ns",
            "range": "± 203.7860432305339"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 229939.00416666668,
            "unit": "ns",
            "range": "± 1678.1679072589666"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 65684.21267089844,
            "unit": "ns",
            "range": "± 241.9240443532197"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 161671.88106863838,
            "unit": "ns",
            "range": "± 816.0515079679692"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 162525.71507161457,
            "unit": "ns",
            "range": "± 652.4317043160089"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 6933.796973092215,
            "unit": "ns",
            "range": "± 13.50157069851713"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 80490.22167154947,
            "unit": "ns",
            "range": "± 305.1711504917478"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 110160.87650553386,
            "unit": "ns",
            "range": "± 240.763026072303"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 61403.95865304129,
            "unit": "ns",
            "range": "± 99.44233091820942"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 196762.78060709636,
            "unit": "ns",
            "range": "± 2067.4219263202563"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 184066.57936314173,
            "unit": "ns",
            "range": "± 921.4096337977887"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 193341.49615885416,
            "unit": "ns",
            "range": "± 1237.4129073470044"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 65697.76949055989,
            "unit": "ns",
            "range": "± 183.84086468480155"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 5303.1509943644205,
            "unit": "ns",
            "range": "± 19.15339686644694"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 64882.23741455078,
            "unit": "ns",
            "range": "± 195.1585964317272"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 133259.78642578126,
            "unit": "ns",
            "range": "± 797.76739367642"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 187464.31993815105,
            "unit": "ns",
            "range": "± 817.2185185501592"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744771222843,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 117033.36529947916,
            "unit": "ns",
            "range": "± 528.1289196566064"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 10882.897436959403,
            "unit": "ns",
            "range": "± 39.985350505878934"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11961.869607543946,
            "unit": "ns",
            "range": "± 39.306074566754134"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 13113.608051554362,
            "unit": "ns",
            "range": "± 48.79678161869742"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 15113.617743858924,
            "unit": "ns",
            "range": "± 12.195669872124935"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 13291.76734822591,
            "unit": "ns",
            "range": "± 63.777886985385436"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 13621.99340057373,
            "unit": "ns",
            "range": "± 30.28448332197518"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 14721.859681193035,
            "unit": "ns",
            "range": "± 48.327610254003964"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 16532.333117675782,
            "unit": "ns",
            "range": "± 79.93242348870059"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 13437.7375,
            "unit": "ns",
            "range": "± 53.3868625756201"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 74158.14335123698,
            "unit": "ns",
            "range": "± 401.49927645674035"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15116.420956929525,
            "unit": "ns",
            "range": "± 8.910494053833816"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 68969.26255580357,
            "unit": "ns",
            "range": "± 277.3188613611689"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 67753.9602376302,
            "unit": "ns",
            "range": "± 385.1318073089719"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 6969.600764465332,
            "unit": "ns",
            "range": "± 24.19572466353852"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12339.771733093261,
            "unit": "ns",
            "range": "± 37.24772645197201"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 16032.858109694262,
            "unit": "ns",
            "range": "± 25.066208338603335"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 11348.96311715933,
            "unit": "ns",
            "range": "± 3.9958232489307304"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 68881.4616041917,
            "unit": "ns",
            "range": "± 61.80380800979902"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 69131.20388559195,
            "unit": "ns",
            "range": "± 117.60978152028159"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 71298.32898888222,
            "unit": "ns",
            "range": "± 53.33551083919441"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 12023.595162455242,
            "unit": "ns",
            "range": "± 39.023138652291394"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 5613.73930867513,
            "unit": "ns",
            "range": "± 20.334499494306836"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 11927.774725777763,
            "unit": "ns",
            "range": "± 6.570599473471406"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 13067.995948282878,
            "unit": "ns",
            "range": "± 45.23729683705869"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 16863.858654785156,
            "unit": "ns",
            "range": "± 95.65688581669998"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 132631.4600830078,
            "unit": "ns",
            "range": "± 508.6663934083041"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 49142.21142578125,
            "unit": "ns",
            "range": "± 71.75987603535357"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 73163.71329345703,
            "unit": "ns",
            "range": "± 227.73312886319994"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 125287.2686360677,
            "unit": "ns",
            "range": "± 475.1538243810691"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 168927.66575520832,
            "unit": "ns",
            "range": "± 553.1100712639778"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 88693.41979980469,
            "unit": "ns",
            "range": "± 564.0388672399906"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 137809.98009440105,
            "unit": "ns",
            "range": "± 545.4729396083374"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 135454.03773716517,
            "unit": "ns",
            "range": "± 519.4822713193669"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 218041.28369140625,
            "unit": "ns",
            "range": "± 631.4597876035886"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 88638.68003336589,
            "unit": "ns",
            "range": "± 376.9420826606272"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 247686.5791015625,
            "unit": "ns",
            "range": "± 3615.7280691929654"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 67473.25486246745,
            "unit": "ns",
            "range": "± 65.70635213801177"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 175986.87908528646,
            "unit": "ns",
            "range": "± 1365.745789148731"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 172026.8864839994,
            "unit": "ns",
            "range": "± 1103.8693747460075"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 7023.562296040853,
            "unit": "ns",
            "range": "± 15.607227089805118"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 78088.59175327847,
            "unit": "ns",
            "range": "± 207.56160804955258"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 119395.07808430989,
            "unit": "ns",
            "range": "± 209.54030142889872"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 61466.98212890625,
            "unit": "ns",
            "range": "± 256.6370683658078"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 212859.32713216144,
            "unit": "ns",
            "range": "± 1450.9036294012851"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 198903.718741862,
            "unit": "ns",
            "range": "± 2066.1276799486686"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 206607.88868815106,
            "unit": "ns",
            "range": "± 1560.409209381624"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 63192.00144856771,
            "unit": "ns",
            "range": "± 246.1478416374865"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 5293.081183297293,
            "unit": "ns",
            "range": "± 7.4961556765353965"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 65094.78797607422,
            "unit": "ns",
            "range": "± 286.11591153184986"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 139839.672328404,
            "unit": "ns",
            "range": "± 661.7960425699994"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 234544.19685872397,
            "unit": "ns",
            "range": "± 1086.1909879673492"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 113957.49065290179,
            "unit": "ns",
            "range": "± 580.2093258541561"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 47382.19837646485,
            "unit": "ns",
            "range": "± 163.2138268400981"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 67598.5229695638,
            "unit": "ns",
            "range": "± 141.20529873159794"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 117749.58523995536,
            "unit": "ns",
            "range": "± 371.22708777832867"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 159676.89688546318,
            "unit": "ns",
            "range": "± 175.9267293105766"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 79585.4907749721,
            "unit": "ns",
            "range": "± 230.25450037449312"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 129141.63559570312,
            "unit": "ns",
            "range": "± 579.7626733062534"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 129200.02224121094,
            "unit": "ns",
            "range": "± 672.7990725953886"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 180389.70438639322,
            "unit": "ns",
            "range": "± 677.1770116859574"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 90469.04268704928,
            "unit": "ns",
            "range": "± 226.76010837272904"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 228355.8888346354,
            "unit": "ns",
            "range": "± 4563.935921551474"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 66224.95373535156,
            "unit": "ns",
            "range": "± 62.25986056097331"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 165198.81809779577,
            "unit": "ns",
            "range": "± 1334.3256330370998"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 161896.17579064003,
            "unit": "ns",
            "range": "± 628.9791110091018"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 6981.873313140869,
            "unit": "ns",
            "range": "± 29.893804785994142"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 78793.08500162761,
            "unit": "ns",
            "range": "± 208.9705301476757"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 108047.31775841347,
            "unit": "ns",
            "range": "± 455.86103867825835"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 61944.683866060695,
            "unit": "ns",
            "range": "± 265.9650255393143"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 197072.47454427084,
            "unit": "ns",
            "range": "± 1898.85070814999"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 184518.99256184895,
            "unit": "ns",
            "range": "± 1262.8094686060342"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 194936.55,
            "unit": "ns",
            "range": "± 1813.5202917147456"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 65291.65270996094,
            "unit": "ns",
            "range": "± 26.58514926687502"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 5269.798509870257,
            "unit": "ns",
            "range": "± 11.700933414092944"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 66022.08277587891,
            "unit": "ns",
            "range": "± 73.93896056984538"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 136069.14589146205,
            "unit": "ns",
            "range": "± 661.724162164299"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 187087.49254557292,
            "unit": "ns",
            "range": "± 623.8253771963657"
          }
        ]
      }
    ],
    "Operations.SortedSetOperations (ubuntu-latest  net8.0 Release)": [
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
        "date": 1744676945052,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 154693.58330426898,
            "unit": "ns",
            "range": "± 785.3122322297058"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11510.20500386556,
            "unit": "ns",
            "range": "± 75.3681223218283"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11957.856555175782,
            "unit": "ns",
            "range": "± 100.09551100506482"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 13314.828088124594,
            "unit": "ns",
            "range": "± 16.002762860964896"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 15970.078314208984,
            "unit": "ns",
            "range": "± 129.82963880477433"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 13484.318454197475,
            "unit": "ns",
            "range": "± 109.67892236641154"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 13782.39334564209,
            "unit": "ns",
            "range": "± 77.02938351713848"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 16201.118052555965,
            "unit": "ns",
            "range": "± 17.789715932096026"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 16483.768242972237,
            "unit": "ns",
            "range": "± 78.19052983982937"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 13223.699471028645,
            "unit": "ns",
            "range": "± 60.01161977092887"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 94316.16266338642,
            "unit": "ns",
            "range": "± 271.8582960416687"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 12008.696408081054,
            "unit": "ns",
            "range": "± 59.05480321485295"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 89281.8186819894,
            "unit": "ns",
            "range": "± 502.8395070549072"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 98080.58326416016,
            "unit": "ns",
            "range": "± 685.5735751724209"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 17388.004723685128,
            "unit": "ns",
            "range": "± 49.78069104547876"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12173.06268956111,
            "unit": "ns",
            "range": "± 36.174267301335206"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 15998.349813842773,
            "unit": "ns",
            "range": "± 84.33029564318194"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 11284.305235799153,
            "unit": "ns",
            "range": "± 69.47297991795025"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 90154.06119791667,
            "unit": "ns",
            "range": "± 417.82057659582557"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 91932.29936000278,
            "unit": "ns",
            "range": "± 456.32006675515635"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 91952.58081054688,
            "unit": "ns",
            "range": "± 161.65565640595193"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 12551.919524129231,
            "unit": "ns",
            "range": "± 59.45155453354433"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 13448.248530796596,
            "unit": "ns",
            "range": "± 68.56710477627966"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 12092.175123087565,
            "unit": "ns",
            "range": "± 45.38084549316896"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 13221.026368204753,
            "unit": "ns",
            "range": "± 48.50872274717672"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 16869.877807617188,
            "unit": "ns",
            "range": "± 57.86536621593365"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 173885.39225260416,
            "unit": "ns",
            "range": "± 701.7600440915959"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 52633.799595424105,
            "unit": "ns",
            "range": "± 140.0857792774744"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 88382.6126145583,
            "unit": "ns",
            "range": "± 248.61249494965762"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 124873.04354654948,
            "unit": "ns",
            "range": "± 664.765758877735"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 189116.9824393136,
            "unit": "ns",
            "range": "± 653.0725220918151"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 111707.74388020833,
            "unit": "ns",
            "range": "± 439.0137395834398"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 138088.40548502604,
            "unit": "ns",
            "range": "± 607.1445872841755"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 140077.0795200893,
            "unit": "ns",
            "range": "± 529.1285993180575"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 245535.69827706474,
            "unit": "ns",
            "range": "± 1629.173769418895"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 107473.69096272787,
            "unit": "ns",
            "range": "± 634.8334286715548"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 319656.201564623,
            "unit": "ns",
            "range": "± 7791.902471070291"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 61812.44635416667,
            "unit": "ns",
            "range": "± 133.62924028898618"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 205994.37826334636,
            "unit": "ns",
            "range": "± 1471.3850253472347"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 205272.78030598958,
            "unit": "ns",
            "range": "± 1037.008785721687"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 17222.61590169271,
            "unit": "ns",
            "range": "± 62.94993190388665"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 85311.0533203125,
            "unit": "ns",
            "range": "± 497.0928433803928"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 136910.11233956474,
            "unit": "ns",
            "range": "± 285.6248047505812"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 58597.62814534505,
            "unit": "ns",
            "range": "± 233.7539062946468"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 245017.4455871582,
            "unit": "ns",
            "range": "± 3546.7764378699835"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 233331.10747070314,
            "unit": "ns",
            "range": "± 2119.08127617243"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 234884.44641927083,
            "unit": "ns",
            "range": "± 2157.5185090799246"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 59122.687526157926,
            "unit": "ns",
            "range": "± 228.8243286090547"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 13789.433396402996,
            "unit": "ns",
            "range": "± 35.665675432582255"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 62281.332682291664,
            "unit": "ns",
            "range": "± 260.00896187747907"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 141043.54486955915,
            "unit": "ns",
            "range": "± 607.0643718188896"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 246931.2639347957,
            "unit": "ns",
            "range": "± 856.1389324543253"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 159375.41276041666,
            "unit": "ns",
            "range": "± 1264.436202432642"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 54871.867671712236,
            "unit": "ns",
            "range": "± 128.2040848186631"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 81272.85063069662,
            "unit": "ns",
            "range": "± 370.7669326829632"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 126077.87854817709,
            "unit": "ns",
            "range": "± 377.5587023954319"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 179073.30643136162,
            "unit": "ns",
            "range": "± 601.9668888560184"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 103977.78955078125,
            "unit": "ns",
            "range": "± 219.9892779952617"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 144377.17488606772,
            "unit": "ns",
            "range": "± 554.8688950495253"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 125739.83710588727,
            "unit": "ns",
            "range": "± 645.1294275397604"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 189181.94008789063,
            "unit": "ns",
            "range": "± 842.2656968839466"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 107753.4471110026,
            "unit": "ns",
            "range": "± 645.5422937965702"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 286266.3719308036,
            "unit": "ns",
            "range": "± 1965.9638904596763"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 63048.46249186198,
            "unit": "ns",
            "range": "± 242.8247060602372"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 191861.01779785156,
            "unit": "ns",
            "range": "± 1616.7488943335268"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 191457.94593098958,
            "unit": "ns",
            "range": "± 834.6183641931718"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 17298.298461914062,
            "unit": "ns",
            "range": "± 96.82616002815064"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 84621.11263020833,
            "unit": "ns",
            "range": "± 359.54910740790376"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 128789.65559895833,
            "unit": "ns",
            "range": "± 1701.5989439459315"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 58829.03346470424,
            "unit": "ns",
            "range": "± 157.1941938325726"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 232489.34409877233,
            "unit": "ns",
            "range": "± 882.0436233995298"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 212722.51166992186,
            "unit": "ns",
            "range": "± 2700.8974124524825"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 216936.82463191106,
            "unit": "ns",
            "range": "± 1585.5577080709818"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 61695.08926595052,
            "unit": "ns",
            "range": "± 351.6313510848626"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 13274.436470540364,
            "unit": "ns",
            "range": "± 47.978407298259306"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 65776.28087565103,
            "unit": "ns",
            "range": "± 392.21600860156013"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 133101.3436279297,
            "unit": "ns",
            "range": "± 590.2891679880767"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 200224.49893624443,
            "unit": "ns",
            "range": "± 716.3153585988094"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744771251783,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 164182.80438639323,
            "unit": "ns",
            "range": "± 884.7920985312918"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11520.198342459542,
            "unit": "ns",
            "range": "± 55.2698400858188"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11822.246052668645,
            "unit": "ns",
            "range": "± 35.84009571642649"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 12959.43207804362,
            "unit": "ns",
            "range": "± 10.560044954074888"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 15285.258024088542,
            "unit": "ns",
            "range": "± 21.9329422681614"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 13427.461655752999,
            "unit": "ns",
            "range": "± 34.40871013960196"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 14300.512017822266,
            "unit": "ns",
            "range": "± 71.58883568193035"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 15089.884884643554,
            "unit": "ns",
            "range": "± 51.76442986732171"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 16521.091376671426,
            "unit": "ns",
            "range": "± 30.07124034789234"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 13234.664080810548,
            "unit": "ns",
            "range": "± 40.24864035311927"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 97481.96674455915,
            "unit": "ns",
            "range": "± 367.67789445677755"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 12105.002586873372,
            "unit": "ns",
            "range": "± 46.37447046594764"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 91951.33319963727,
            "unit": "ns",
            "range": "± 250.0129626317658"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 90375.64739990234,
            "unit": "ns",
            "range": "± 495.08926916060295"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 17398.798380533855,
            "unit": "ns",
            "range": "± 133.22561565765702"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12191.64574432373,
            "unit": "ns",
            "range": "± 64.40219397724381"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 16058.78332737514,
            "unit": "ns",
            "range": "± 82.3669455273698"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 11261.301088605609,
            "unit": "ns",
            "range": "± 48.78639540754293"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 94955.80872395834,
            "unit": "ns",
            "range": "± 519.2644043842307"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 91543.8657836914,
            "unit": "ns",
            "range": "± 381.5140949301952"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 91724.97743577223,
            "unit": "ns",
            "range": "± 287.9450896006453"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 12083.699407724234,
            "unit": "ns",
            "range": "± 25.157160783680787"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 13433.552811686199,
            "unit": "ns",
            "range": "± 76.20763450941051"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 11933.390722147624,
            "unit": "ns",
            "range": "± 67.80022501635277"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 13248.873964436849,
            "unit": "ns",
            "range": "± 38.28401293138709"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 16805.373683384485,
            "unit": "ns",
            "range": "± 14.761368796111931"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 179176.72794015068,
            "unit": "ns",
            "range": "± 519.3900575524846"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 53470.75318603516,
            "unit": "ns",
            "range": "± 235.53400700411723"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 84605.66872558594,
            "unit": "ns",
            "range": "± 386.4068808083814"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 127007.39977213541,
            "unit": "ns",
            "range": "± 720.6718329686828"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 187570.93198939733,
            "unit": "ns",
            "range": "± 655.226631980661"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 114633.83916829427,
            "unit": "ns",
            "range": "± 426.34762558078177"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 137159.1522216797,
            "unit": "ns",
            "range": "± 689.0517798972712"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 141912.34744698662,
            "unit": "ns",
            "range": "± 815.3655278153906"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 246237.08403320314,
            "unit": "ns",
            "range": "± 2258.2699529086353"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 104251.06721714565,
            "unit": "ns",
            "range": "± 337.47735921023065"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 305872.07708333334,
            "unit": "ns",
            "range": "± 5680.700075096813"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 71505.1530843099,
            "unit": "ns",
            "range": "± 239.24560367463178"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 203649.12370954241,
            "unit": "ns",
            "range": "± 902.1382212498734"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 206889.19459635418,
            "unit": "ns",
            "range": "± 1515.3504729484432"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 17210.526780192056,
            "unit": "ns",
            "range": "± 99.29022059091582"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 79936.59063313802,
            "unit": "ns",
            "range": "± 391.143097908753"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 138455.3479191707,
            "unit": "ns",
            "range": "± 351.95204921356833"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 60133.073564801896,
            "unit": "ns",
            "range": "± 311.8814745249064"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 244512.2608173077,
            "unit": "ns",
            "range": "± 1956.7332778289795"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 225721.64524739582,
            "unit": "ns",
            "range": "± 3064.8842941773532"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 234650.67587890624,
            "unit": "ns",
            "range": "± 3242.601761986636"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 62227.34185321514,
            "unit": "ns",
            "range": "± 426.549248591441"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 13269.511560058594,
            "unit": "ns",
            "range": "± 40.890954713629064"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 70437.72125651041,
            "unit": "ns",
            "range": "± 299.60113429743865"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 146532.011328125,
            "unit": "ns",
            "range": "± 638.8583984791632"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 249432.38251953124,
            "unit": "ns",
            "range": "± 2293.4233210370035"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 159166.1466796875,
            "unit": "ns",
            "range": "± 1010.2693569531492"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 53375.9626159668,
            "unit": "ns",
            "range": "± 150.26701950493506"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 80189.32084960937,
            "unit": "ns",
            "range": "± 323.7871332606976"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 115485.69899088542,
            "unit": "ns",
            "range": "± 356.17951791622767"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 185562.79132952009,
            "unit": "ns",
            "range": "± 931.353033040255"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 102766.28524983724,
            "unit": "ns",
            "range": "± 524.4157558545859"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 140640.86982421874,
            "unit": "ns",
            "range": "± 1128.9685171971507"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 126725.06478445871,
            "unit": "ns",
            "range": "± 472.2400957321881"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 193291.1791341146,
            "unit": "ns",
            "range": "± 784.7486820271914"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 102481.4873046875,
            "unit": "ns",
            "range": "± 433.07618034221247"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 289376.74853515625,
            "unit": "ns",
            "range": "± 2659.9016609109112"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 63326.278346470426,
            "unit": "ns",
            "range": "± 248.3428804206542"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 198041.12636021205,
            "unit": "ns",
            "range": "± 1140.9968286802768"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 195449.4822265625,
            "unit": "ns",
            "range": "± 1319.7158132005707"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 17285.540188598632,
            "unit": "ns",
            "range": "± 63.77742939011223"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 82642.81475423177,
            "unit": "ns",
            "range": "± 385.7515506439445"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 127400.21751051683,
            "unit": "ns",
            "range": "± 531.3512417014958"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 61934.14855957031,
            "unit": "ns",
            "range": "± 428.4791327640951"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 227255.39762995794,
            "unit": "ns",
            "range": "± 1358.805198677845"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 213655.08530273437,
            "unit": "ns",
            "range": "± 2185.9322413780337"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 221191.6674107143,
            "unit": "ns",
            "range": "± 1574.3447688255594"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 61309.43583984375,
            "unit": "ns",
            "range": "± 524.1350909893783"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 13208.349930826824,
            "unit": "ns",
            "range": "± 41.631227436232024"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 63594.127005440845,
            "unit": "ns",
            "range": "± 306.9078461172607"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 137338.15774739583,
            "unit": "ns",
            "range": "± 749.8809010323138"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 211219.41022600446,
            "unit": "ns",
            "range": "± 471.5349121355986"
          }
        ]
      }
    ],
    "Operations.SortedSetOperations (windows-latest  net9.0 Release)": [
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
        "date": 1744677339749,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 106282.8515625,
            "unit": "ns",
            "range": "± 187.49769261620713"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11517.84434000651,
            "unit": "ns",
            "range": "± 48.01516230121196"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11672.181584284855,
            "unit": "ns",
            "range": "± 14.979003221337539"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 14748.59634399414,
            "unit": "ns",
            "range": "± 31.938430993468803"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 21304.776000976562,
            "unit": "ns",
            "range": "± 74.57190004120233"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 15841.450805664062,
            "unit": "ns",
            "range": "± 29.39013527930579"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 17885.865565708704,
            "unit": "ns",
            "range": "± 23.34423231771431"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 22599.952697753906,
            "unit": "ns",
            "range": "± 17.16966769622225"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 26548.433358328683,
            "unit": "ns",
            "range": "± 68.19180202674725"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 16472.36562093099,
            "unit": "ns",
            "range": "± 92.40405114837014"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 73505.30436197917,
            "unit": "ns",
            "range": "± 372.48184570256717"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15523.71128627232,
            "unit": "ns",
            "range": "± 11.54728076623877"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 68454.25790640023,
            "unit": "ns",
            "range": "± 71.785982331743"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 64635.67667643229,
            "unit": "ns",
            "range": "± 188.25192880996315"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 6175.58474222819,
            "unit": "ns",
            "range": "± 12.179008551536159"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12938.959299723307,
            "unit": "ns",
            "range": "± 28.343367792610056"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 24588.716997419084,
            "unit": "ns",
            "range": "± 16.7964467017315"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 12271.024649483817,
            "unit": "ns",
            "range": "± 10.820009541099967"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 75638.48266601562,
            "unit": "ns",
            "range": "± 193.37638037079083"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 70579.38701923077,
            "unit": "ns",
            "range": "± 63.54786971666894"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 71607.42553710938,
            "unit": "ns",
            "range": "± 108.55170228822843"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 14311.24028523763,
            "unit": "ns",
            "range": "± 40.78186818041299"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 4443.603897094727,
            "unit": "ns",
            "range": "± 6.654768648791162"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 14596.500161977914,
            "unit": "ns",
            "range": "± 10.257388761929501"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 15120.041329520089,
            "unit": "ns",
            "range": "± 23.618009210212062"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 27632.68280029297,
            "unit": "ns",
            "range": "± 49.80941400085802"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 121690.87611607143,
            "unit": "ns",
            "range": "± 482.9543224185577"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 34971.65034367488,
            "unit": "ns",
            "range": "± 47.65802933705237"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 56758.456186147836,
            "unit": "ns",
            "range": "± 155.4789434901025"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 97282.109375,
            "unit": "ns",
            "range": "± 276.9436623368909"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 141717.21028645834,
            "unit": "ns",
            "range": "± 738.6793838645357"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 87251.7110188802,
            "unit": "ns",
            "range": "± 334.5997826944132"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 107238.89892578125,
            "unit": "ns",
            "range": "± 268.27965640461144"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 112608.60421316964,
            "unit": "ns",
            "range": "± 198.99137653568388"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 190509.90365835337,
            "unit": "ns",
            "range": "± 377.0962181462273"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 76063.19295247395,
            "unit": "ns",
            "range": "± 405.97389879809776"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 237238.41634114584,
            "unit": "ns",
            "range": "± 2976.4736887733543"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 56682.385689871655,
            "unit": "ns",
            "range": "± 59.58401529435772"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 164845.63860212054,
            "unit": "ns",
            "range": "± 712.0045184839316"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 161891.29450871394,
            "unit": "ns",
            "range": "± 503.5478767995544"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 6268.720601399739,
            "unit": "ns",
            "range": "± 13.246776639807905"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 69256.79361979167,
            "unit": "ns",
            "range": "± 177.86466819390407"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 106438.03335336539,
            "unit": "ns",
            "range": "± 689.9763301638576"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 48122.33846028646,
            "unit": "ns",
            "range": "± 151.4142429053964"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 215051.94091796875,
            "unit": "ns",
            "range": "± 1016.1468405672769"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 187058.1827799479,
            "unit": "ns",
            "range": "± 1223.0685126955875"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 202101.01888020834,
            "unit": "ns",
            "range": "± 1766.0964363070545"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 50895.99060058594,
            "unit": "ns",
            "range": "± 171.24502737581915"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 4413.046455383301,
            "unit": "ns",
            "range": "± 4.9547697311952605"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 53917.89812360491,
            "unit": "ns",
            "range": "± 66.1539064862136"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 109774.51538085938,
            "unit": "ns",
            "range": "± 439.7479182125293"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 201630.25184044472,
            "unit": "ns",
            "range": "± 558.4897393570084"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 106507.28323800223,
            "unit": "ns",
            "range": "± 101.18433058588276"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 33998.95955403646,
            "unit": "ns",
            "range": "± 38.47769664801516"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 58478.307669503345,
            "unit": "ns",
            "range": "± 239.26980657429039"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 94769.51822916667,
            "unit": "ns",
            "range": "± 186.34530505302553"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 140189.1398111979,
            "unit": "ns",
            "range": "± 145.9029162148418"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 81233.70012555804,
            "unit": "ns",
            "range": "± 119.78026231180537"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 103726.37590680804,
            "unit": "ns",
            "range": "± 184.10077425509132"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 114474.30855887277,
            "unit": "ns",
            "range": "± 180.93409262319867"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 157048.94287109375,
            "unit": "ns",
            "range": "± 881.5261924562523"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 76909.34099469866,
            "unit": "ns",
            "range": "± 314.55881366394664"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 216790.17903645834,
            "unit": "ns",
            "range": "± 3153.6152285804274"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 57334.77924053486,
            "unit": "ns",
            "range": "± 37.506671496993185"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 154966.1474609375,
            "unit": "ns",
            "range": "± 479.15109646814847"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 152942.64962332588,
            "unit": "ns",
            "range": "± 701.2883034439416"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 6147.076034545898,
            "unit": "ns",
            "range": "± 15.785728954554653"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 67494.16765485491,
            "unit": "ns",
            "range": "± 266.1281899584204"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 99891.35366586539,
            "unit": "ns",
            "range": "± 385.7167401312996"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 49975.32741001674,
            "unit": "ns",
            "range": "± 91.04841286076531"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 192873.9013671875,
            "unit": "ns",
            "range": "± 462.49756723188403"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 182926.9881184896,
            "unit": "ns",
            "range": "± 1004.7951650770934"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 198919.52473958334,
            "unit": "ns",
            "range": "± 1064.3646316027578"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 51485.028076171875,
            "unit": "ns",
            "range": "± 99.03669020949704"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 4427.260644095285,
            "unit": "ns",
            "range": "± 7.6211467923836445"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 55420.88358561198,
            "unit": "ns",
            "range": "± 67.90162039881727"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 105171.73374720982,
            "unit": "ns",
            "range": "± 225.63643343342622"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 176719.09743088944,
            "unit": "ns",
            "range": "± 283.1046282001619"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744771624871,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 105016.44723074777,
            "unit": "ns",
            "range": "± 150.58730328385082"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11133.64974975586,
            "unit": "ns",
            "range": "± 34.9949696454899"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11920.278676350912,
            "unit": "ns",
            "range": "± 63.02249648388133"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 15003.046671549479,
            "unit": "ns",
            "range": "± 37.33755559079219"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 21143.753487723214,
            "unit": "ns",
            "range": "± 53.36394819138345"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 15794.031270345053,
            "unit": "ns",
            "range": "± 34.68944593632187"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 17840.409029447117,
            "unit": "ns",
            "range": "± 13.804767670400759"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 22738.66941011869,
            "unit": "ns",
            "range": "± 16.888813674969725"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 26563.749577448918,
            "unit": "ns",
            "range": "± 48.488173854885225"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 16252.905476888021,
            "unit": "ns",
            "range": "± 82.59188977860663"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 73740.66913311298,
            "unit": "ns",
            "range": "± 144.40420560106298"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15485.863138834635,
            "unit": "ns",
            "range": "± 10.203246088866543"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 67262.55777994792,
            "unit": "ns",
            "range": "± 58.05353570320338"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 65792.35432942708,
            "unit": "ns",
            "range": "± 106.91861328659976"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 6348.879132952009,
            "unit": "ns",
            "range": "± 19.80305850834659"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12923.537227085659,
            "unit": "ns",
            "range": "± 10.267975709993118"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 24388.05193219866,
            "unit": "ns",
            "range": "± 21.944257850203346"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 11986.936514718192,
            "unit": "ns",
            "range": "± 13.455044805631156"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 74549.87284342448,
            "unit": "ns",
            "range": "± 53.50678451469039"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 71536.39103816106,
            "unit": "ns",
            "range": "± 86.98977610030616"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 72732.67037527902,
            "unit": "ns",
            "range": "± 89.47138714998273"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 14145.108904157367,
            "unit": "ns",
            "range": "± 34.611184651102676"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 4514.902768816267,
            "unit": "ns",
            "range": "± 13.93554808957477"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 14440.694754464286,
            "unit": "ns",
            "range": "± 10.380556471129701"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 15261.120198567709,
            "unit": "ns",
            "range": "± 30.042603185623427"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 27772.261657714844,
            "unit": "ns",
            "range": "± 18.600396244145653"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 122742.67252604167,
            "unit": "ns",
            "range": "± 487.3409605565948"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 37400.57242257254,
            "unit": "ns",
            "range": "± 106.97820035711447"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 56208.46862792969,
            "unit": "ns",
            "range": "± 95.81245298767057"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 101594.8124186198,
            "unit": "ns",
            "range": "± 416.7277212194"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 138006.34033203125,
            "unit": "ns",
            "range": "± 756.8852752608175"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 86766.04919433594,
            "unit": "ns",
            "range": "± 152.93528761265992"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 106443.5811360677,
            "unit": "ns",
            "range": "± 442.519841781355"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 109012.89860652044,
            "unit": "ns",
            "range": "± 284.56469813036585"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 187314.9503580729,
            "unit": "ns",
            "range": "± 1366.6586854492025"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 74206.55604771206,
            "unit": "ns",
            "range": "± 232.35273469410234"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 239116.76199776787,
            "unit": "ns",
            "range": "± 4033.1327686048585"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 55380.221322866586,
            "unit": "ns",
            "range": "± 53.41667843313821"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 169146.81315104166,
            "unit": "ns",
            "range": "± 1286.654706421001"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 158356.8425105168,
            "unit": "ns",
            "range": "± 813.4299976904342"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 6288.575719197591,
            "unit": "ns",
            "range": "± 12.37814143372263"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 68875.14119466145,
            "unit": "ns",
            "range": "± 308.9765625041154"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 104919.38127790179,
            "unit": "ns",
            "range": "± 516.1343363154729"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 49780.932181222095,
            "unit": "ns",
            "range": "± 178.35654369804595"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 200457.98990885416,
            "unit": "ns",
            "range": "± 1574.49247102801"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 184406.36881510416,
            "unit": "ns",
            "range": "± 1261.3478639543298"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 206841.52657645088,
            "unit": "ns",
            "range": "± 1766.5948813338048"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 50279.62079729353,
            "unit": "ns",
            "range": "± 246.83507009129372"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 4487.656656901042,
            "unit": "ns",
            "range": "± 8.62991646136833"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 54092.029244559155,
            "unit": "ns",
            "range": "± 112.93939351031491"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 108054.45556640625,
            "unit": "ns",
            "range": "± 561.4598368765203"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 200735.57303292412,
            "unit": "ns",
            "range": "± 797.3553615887399"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 107314.6257672991,
            "unit": "ns",
            "range": "± 339.570976319162"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 34781.47420247396,
            "unit": "ns",
            "range": "± 103.20854447495087"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 59315.78816731771,
            "unit": "ns",
            "range": "± 264.8596358012636"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 90918.40983072917,
            "unit": "ns",
            "range": "± 430.7628343561139"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 132832.9638671875,
            "unit": "ns",
            "range": "± 267.34725733266475"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 80819.49986049107,
            "unit": "ns",
            "range": "± 306.65865580986264"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 101757.32788085938,
            "unit": "ns",
            "range": "± 754.1729010104374"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 105893.1201171875,
            "unit": "ns",
            "range": "± 486.0808569930775"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 152992.05510066106,
            "unit": "ns",
            "range": "± 1112.1442101511145"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 75140.32156808036,
            "unit": "ns",
            "range": "± 541.8493593338577"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 214814.8095703125,
            "unit": "ns",
            "range": "± 1629.7192693836719"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 56273.58601888021,
            "unit": "ns",
            "range": "± 140.51949749181398"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 154925.48990885416,
            "unit": "ns",
            "range": "± 755.2610572250194"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 156288.8671875,
            "unit": "ns",
            "range": "± 921.4893636979876"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 6306.911112467448,
            "unit": "ns",
            "range": "± 20.122485066107274"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 66167.10205078125,
            "unit": "ns",
            "range": "± 279.1964500357441"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 98995.2880859375,
            "unit": "ns",
            "range": "± 395.6333638566988"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 49019.39514160156,
            "unit": "ns",
            "range": "± 157.71651205456848"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 194337.97200520834,
            "unit": "ns",
            "range": "± 660.8104372175617"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 172697.04415457588,
            "unit": "ns",
            "range": "± 941.6579299156859"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 195085.84187825522,
            "unit": "ns",
            "range": "± 463.5597675404699"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 50292.85191127232,
            "unit": "ns",
            "range": "± 126.60798561453008"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 4477.075371375451,
            "unit": "ns",
            "range": "± 7.551219321565645"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 53523.01810128348,
            "unit": "ns",
            "range": "± 107.321554020676"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 103409.0234375,
            "unit": "ns",
            "range": "± 211.6286655308467"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 163458.14208984375,
            "unit": "ns",
            "range": "± 1366.5278786174276"
          }
        ]
      }
    ],
    "Operations.SortedSetOperations (windows-latest  net8.0 Release)": [
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
        "date": 1744677374832,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 131476.74466646634,
            "unit": "ns",
            "range": "± 378.0565749823883"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 11985.411482590895,
            "unit": "ns",
            "range": "± 52.797407443808645"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11860.427739070012,
            "unit": "ns",
            "range": "± 10.590453657831713"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 16193.479919433594,
            "unit": "ns",
            "range": "± 14.665071616367065"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 23201.845296223957,
            "unit": "ns",
            "range": "± 25.885128102736807"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 17542.914757361777,
            "unit": "ns",
            "range": "± 12.60098925719503"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 18534.149605887276,
            "unit": "ns",
            "range": "± 13.451581672059797"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 23906.624930245536,
            "unit": "ns",
            "range": "± 18.07974328465028"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 28147.261919294084,
            "unit": "ns",
            "range": "± 27.077769460633945"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 17143.694716233473,
            "unit": "ns",
            "range": "± 25.147374502756325"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 85377.04990931919,
            "unit": "ns",
            "range": "± 192.1651466206296"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15619.612223307291,
            "unit": "ns",
            "range": "± 18.509415158980786"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 75737.41542271206,
            "unit": "ns",
            "range": "± 90.93583066693873"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 74803.35083007812,
            "unit": "ns",
            "range": "± 105.29020903639747"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 13067.813286414514,
            "unit": "ns",
            "range": "± 49.25600677222818"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12958.323465983072,
            "unit": "ns",
            "range": "± 13.150091744525728"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 25822.168731689453,
            "unit": "ns",
            "range": "± 9.252793635216559"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 12286.113848005023,
            "unit": "ns",
            "range": "± 9.942273698970777"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 78152.72639347956,
            "unit": "ns",
            "range": "± 93.18873225395522"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 78350.82920619419,
            "unit": "ns",
            "range": "± 130.34223025131655"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 85529.12034254808,
            "unit": "ns",
            "range": "± 82.78905197916133"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 14758.694047194262,
            "unit": "ns",
            "range": "± 8.755290995490016"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 9444.595387776693,
            "unit": "ns",
            "range": "± 31.158076179713944"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 14853.10309273856,
            "unit": "ns",
            "range": "± 16.951731169822544"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 16339.734976632255,
            "unit": "ns",
            "range": "± 24.136778356011977"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 29752.698843819755,
            "unit": "ns",
            "range": "± 12.050416497472419"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 138276.4404296875,
            "unit": "ns",
            "range": "± 348.50727628114805"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 42088.03452711839,
            "unit": "ns",
            "range": "± 65.01459187650869"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 64267.85191127232,
            "unit": "ns",
            "range": "± 158.10406215876208"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 109374.1666353666,
            "unit": "ns",
            "range": "± 301.8758502676512"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 163244.7452799479,
            "unit": "ns",
            "range": "± 1190.7436150304266"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 97404.36575753348,
            "unit": "ns",
            "range": "± 329.8288938395567"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 123520.65592447917,
            "unit": "ns",
            "range": "± 682.6481114325541"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 127929.93489583333,
            "unit": "ns",
            "range": "± 779.5334678226358"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 215353.81673177084,
            "unit": "ns",
            "range": "± 1016.6955535799019"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 88367.8959773137,
            "unit": "ns",
            "range": "± 270.27619314736455"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 268640.8512369792,
            "unit": "ns",
            "range": "± 2372.874050433114"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 60176.524135044645,
            "unit": "ns",
            "range": "± 177.53195195863856"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 185188.34635416666,
            "unit": "ns",
            "range": "± 423.315270941257"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 167484.65924944196,
            "unit": "ns",
            "range": "± 684.96177312375"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 12971.824086507162,
            "unit": "ns",
            "range": "± 38.219691582766416"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 78642.73420061384,
            "unit": "ns",
            "range": "± 358.4195581769203"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 119748.38518415179,
            "unit": "ns",
            "range": "± 784.8271210465971"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 58833.207194010414,
            "unit": "ns",
            "range": "± 603.5597466248315"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 214826.4927455357,
            "unit": "ns",
            "range": "± 1660.7731479763927"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 223767.9728190104,
            "unit": "ns",
            "range": "± 1519.2124538052303"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 220578.9453125,
            "unit": "ns",
            "range": "± 2032.6041943908608"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 53676.11432756697,
            "unit": "ns",
            "range": "± 129.60445357994604"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 9355.870666503906,
            "unit": "ns",
            "range": "± 21.84675458455314"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 59983.23843819754,
            "unit": "ns",
            "range": "± 158.45639183306284"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 128010.71589543269,
            "unit": "ns",
            "range": "± 769.3506856788741"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 223103.24009486608,
            "unit": "ns",
            "range": "± 709.3830845770972"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 126621.30314753606,
            "unit": "ns",
            "range": "± 195.0967877852146"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 41006.968994140625,
            "unit": "ns",
            "range": "± 103.63829365146279"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 64949.908447265625,
            "unit": "ns",
            "range": "± 266.4611570047889"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 101486.47112165179,
            "unit": "ns",
            "range": "± 217.2539274375825"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 152681.82198660713,
            "unit": "ns",
            "range": "± 513.5668871461903"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 89158.73413085938,
            "unit": "ns",
            "range": "± 131.44981249984767"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 124831.68381911058,
            "unit": "ns",
            "range": "± 402.2005153558953"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 118321.84099469866,
            "unit": "ns",
            "range": "± 268.8194883765329"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 180809.57500751203,
            "unit": "ns",
            "range": "± 393.71103358648287"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 87493.4842936198,
            "unit": "ns",
            "range": "± 288.12934864178044"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 246266.0888671875,
            "unit": "ns",
            "range": "± 1156.564963804622"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 61208.9794921875,
            "unit": "ns",
            "range": "± 116.01835652227989"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 163917.0548502604,
            "unit": "ns",
            "range": "± 734.0672511065893"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 158885.62662760416,
            "unit": "ns",
            "range": "± 891.266116489093"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 12904.52880859375,
            "unit": "ns",
            "range": "± 46.60342320402792"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 76135.3759765625,
            "unit": "ns",
            "range": "± 153.07106995179788"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 112447.24684495192,
            "unit": "ns",
            "range": "± 235.57266046848594"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 54302.8476969401,
            "unit": "ns",
            "range": "± 98.16504625224309"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 200637.60892427884,
            "unit": "ns",
            "range": "± 607.6395183033646"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 212142.3828125,
            "unit": "ns",
            "range": "± 1307.3724619403745"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 215564.1592172476,
            "unit": "ns",
            "range": "± 1132.6914356747993"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 59635.74698311942,
            "unit": "ns",
            "range": "± 192.04269006497597"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 9336.121259416852,
            "unit": "ns",
            "range": "± 28.418239326473888"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 61783.772786458336,
            "unit": "ns",
            "range": "± 90.22093841721413"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 123605.65999348958,
            "unit": "ns",
            "range": "± 251.51644705186206"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 185628.22591145834,
            "unit": "ns",
            "range": "± 589.1912409891846"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pon.vijaynirmal@outlook.com",
            "name": "Vijay Nirmal",
            "username": "Vijay-Nirmal"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8ae8fb42c702ca80605195947629be3598d590af",
          "message": "Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commands (#1162)\n\n* Fix wrong type and non existing key issues in S/Z UNION, DIFF, INTER commants\n\n* Fixed review comment\n\n---------\n\nCo-authored-by: Badrish Chandramouli <badrishc@microsoft.com>",
          "timestamp": "2025-04-15T19:09:29-07:00",
          "tree_id": "6b771dd93a997a56b2e58858648cbad33b02cee3",
          "url": "https://github.com/microsoft/garnet/commit/8ae8fb42c702ca80605195947629be3598d590af"
        },
        "date": 1744771766281,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: ACL)",
            "value": 132104.55496651787,
            "unit": "ns",
            "range": "± 493.35215998735976"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: ACL)",
            "value": 12517.491251627604,
            "unit": "ns",
            "range": "± 15.644117515653406"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: ACL)",
            "value": 11972.40219116211,
            "unit": "ns",
            "range": "± 18.905945162792236"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: ACL)",
            "value": 16144.550984700521,
            "unit": "ns",
            "range": "± 20.458181356578766"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: ACL)",
            "value": 23212.300763811385,
            "unit": "ns",
            "range": "± 13.277655565432058"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: ACL)",
            "value": 16793.28104654948,
            "unit": "ns",
            "range": "± 13.881291369930215"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: ACL)",
            "value": 17981.833975655692,
            "unit": "ns",
            "range": "± 14.755584679339243"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: ACL)",
            "value": 24783.185323079426,
            "unit": "ns",
            "range": "± 12.695417753069746"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: ACL)",
            "value": 28013.426426478796,
            "unit": "ns",
            "range": "± 24.471149860573053"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: ACL)",
            "value": 16515.65419514974,
            "unit": "ns",
            "range": "± 22.061446264081617"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: ACL)",
            "value": 82499.74283854167,
            "unit": "ns",
            "range": "± 162.81471218164418"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: ACL)",
            "value": 15599.654642740885,
            "unit": "ns",
            "range": "± 8.85419550892329"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: ACL)",
            "value": 72123.03553989956,
            "unit": "ns",
            "range": "± 166.16503615834102"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: ACL)",
            "value": 77067.18662806919,
            "unit": "ns",
            "range": "± 118.63323899190347"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: ACL)",
            "value": 12975.239562988281,
            "unit": "ns",
            "range": "± 18.77719404851087"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: ACL)",
            "value": 12967.428806849888,
            "unit": "ns",
            "range": "± 9.445370806821648"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: ACL)",
            "value": 26525.169677734375,
            "unit": "ns",
            "range": "± 26.12432425295462"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: ACL)",
            "value": 12362.337036132812,
            "unit": "ns",
            "range": "± 16.364209409865307"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: ACL)",
            "value": 78445.73625837054,
            "unit": "ns",
            "range": "± 160.3747947951441"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: ACL)",
            "value": 82279.38802083333,
            "unit": "ns",
            "range": "± 117.05847802564473"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: ACL)",
            "value": 80531.10613141741,
            "unit": "ns",
            "range": "± 156.89575960690297"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: ACL)",
            "value": 14944.78487287249,
            "unit": "ns",
            "range": "± 18.897317614906097"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: ACL)",
            "value": 9236.388600667318,
            "unit": "ns",
            "range": "± 20.12990389633334"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: ACL)",
            "value": 14811.155395507812,
            "unit": "ns",
            "range": "± 12.604856746177537"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: ACL)",
            "value": 15750.415860689604,
            "unit": "ns",
            "range": "± 14.71688539673326"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: ACL)",
            "value": 29523.731486002605,
            "unit": "ns",
            "range": "± 9.687376669455718"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: AOF)",
            "value": 140977.0304361979,
            "unit": "ns",
            "range": "± 305.6513769815644"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: AOF)",
            "value": 40257.28861490885,
            "unit": "ns",
            "range": "± 72.54389271854839"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: AOF)",
            "value": 64809.3741280692,
            "unit": "ns",
            "range": "± 107.94766762588318"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: AOF)",
            "value": 107413.17260742188,
            "unit": "ns",
            "range": "± 390.5679744730698"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: AOF)",
            "value": 161648.41634114584,
            "unit": "ns",
            "range": "± 1135.8973862345417"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: AOF)",
            "value": 97816.01603190105,
            "unit": "ns",
            "range": "± 324.22041404634126"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: AOF)",
            "value": 124265.11800130208,
            "unit": "ns",
            "range": "± 665.0447865197065"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: AOF)",
            "value": 136202.15541294642,
            "unit": "ns",
            "range": "± 771.1520620431969"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: AOF)",
            "value": 214360.42949969953,
            "unit": "ns",
            "range": "± 634.3116843273655"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: AOF)",
            "value": 92748.62752278645,
            "unit": "ns",
            "range": "± 268.8927553078237"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: AOF)",
            "value": 271700.75334821426,
            "unit": "ns",
            "range": "± 1705.7535275741902"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: AOF)",
            "value": 60096.563720703125,
            "unit": "ns",
            "range": "± 111.24846165478706"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: AOF)",
            "value": 171393.54945591517,
            "unit": "ns",
            "range": "± 935.1102779279437"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: AOF)",
            "value": 171561.4755483774,
            "unit": "ns",
            "range": "± 972.3933640415513"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: AOF)",
            "value": 13029.606410435268,
            "unit": "ns",
            "range": "± 13.803953283594657"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: AOF)",
            "value": 72506.95364815848,
            "unit": "ns",
            "range": "± 300.908059498675"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: AOF)",
            "value": 119638.22195870536,
            "unit": "ns",
            "range": "± 649.7578209756715"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: AOF)",
            "value": 57042.781575520836,
            "unit": "ns",
            "range": "± 83.1189962057478"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: AOF)",
            "value": 224484.81119791666,
            "unit": "ns",
            "range": "± 788.6335372846794"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: AOF)",
            "value": 213092.95572916666,
            "unit": "ns",
            "range": "± 1356.402981101347"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: AOF)",
            "value": 219337.94759114584,
            "unit": "ns",
            "range": "± 1504.043969599762"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: AOF)",
            "value": 54417.464192708336,
            "unit": "ns",
            "range": "± 144.9075883438955"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: AOF)",
            "value": 9308.901868547711,
            "unit": "ns",
            "range": "± 20.452378625579275"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: AOF)",
            "value": 60149.1694523738,
            "unit": "ns",
            "range": "± 94.12808253802862"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: AOF)",
            "value": 126760.35319010417,
            "unit": "ns",
            "range": "± 787.1844464183001"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: AOF)",
            "value": 223685.60442243304,
            "unit": "ns",
            "range": "± 640.5420346855223"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZAddRem(Params: None)",
            "value": 126457.97688802083,
            "unit": "ns",
            "range": "± 238.39763103526778"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCard(Params: None)",
            "value": 41318.75774676983,
            "unit": "ns",
            "range": "± 57.086343704399056"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZCount(Params: None)",
            "value": 65939.12682166466,
            "unit": "ns",
            "range": "± 138.63011381939845"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiff(Params: None)",
            "value": 103592.1219889323,
            "unit": "ns",
            "range": "± 224.86911672183265"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZDiffStore(Params: None)",
            "value": 166512.8361002604,
            "unit": "ns",
            "range": "± 326.72710995530315"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZIncrby(Params: None)",
            "value": 91827.03857421875,
            "unit": "ns",
            "range": "± 221.18253388556124"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInter(Params: None)",
            "value": 120712.4755859375,
            "unit": "ns",
            "range": "± 331.28747808377074"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterCard(Params: None)",
            "value": 125300.48828125,
            "unit": "ns",
            "range": "± 244.0131101038673"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZInterStore(Params: None)",
            "value": 175216.3295200893,
            "unit": "ns",
            "range": "± 336.83441147432325"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZLexCount(Params: None)",
            "value": 94560.64982096355,
            "unit": "ns",
            "range": "± 300.29747473643823"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMPop(Params: None)",
            "value": 250947.890625,
            "unit": "ns",
            "range": "± 2797.0318189468467"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZMScore(Params: None)",
            "value": 61209.403076171875,
            "unit": "ns",
            "range": "± 191.87633977016657"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMax(Params: None)",
            "value": 172369.19108072916,
            "unit": "ns",
            "range": "± 588.0039428842724"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZPopMin(Params: None)",
            "value": 167497.67543247767,
            "unit": "ns",
            "range": "± 315.0660917411663"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRandMember(Params: None)",
            "value": 12829.634958902994,
            "unit": "ns",
            "range": "± 20.99717482342623"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRange(Params: None)",
            "value": 73669.85473632812,
            "unit": "ns",
            "range": "± 193.26819263966686"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRangeStore(Params: None)",
            "value": 113390.92548076923,
            "unit": "ns",
            "range": "± 218.55790363649126"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRank(Params: None)",
            "value": 55739.76989746094,
            "unit": "ns",
            "range": "± 158.0942666610834"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByLex(Params: None)",
            "value": 197845.61941964287,
            "unit": "ns",
            "range": "± 746.4688228695261"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByRank(Params: None)",
            "value": 211971.94986979166,
            "unit": "ns",
            "range": "± 1318.7907837153175"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRemRangeByScore(Params: None)",
            "value": 226398.2625325521,
            "unit": "ns",
            "range": "± 1156.5907304982643"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZRevRank(Params: None)",
            "value": 55225.557861328125,
            "unit": "ns",
            "range": "± 76.71902014296614"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScan(Params: None)",
            "value": 9343.806355794271,
            "unit": "ns",
            "range": "± 28.943195965599973"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZScore(Params: None)",
            "value": 60387.828776041664,
            "unit": "ns",
            "range": "± 102.60646867048476"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnion(Params: None)",
            "value": 118721.08072916667,
            "unit": "ns",
            "range": "± 455.6277957491351"
          },
          {
            "name": "BDN.benchmark.Operations.SortedSetOperations.ZUnionStore(Params: None)",
            "value": 187026.91731770834,
            "unit": "ns",
            "range": "± 529.6967039716563"
          }
        ]
      }
    ]
  }
}