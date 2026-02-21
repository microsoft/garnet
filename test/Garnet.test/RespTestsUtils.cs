// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test;

public static class RespTestsUtils
{
    /// <summary>
    /// Add object(s) of specified type to database using SE.Redis
    /// </summary>
    /// <param name="db">SE.Redis database wrapper</param>
    /// <param name="objectType">Object type to add</param>
    /// <param name="keys">Array of keys to add</param>
    /// <param name="values">Array of arrays of values to add to each object at key</param>
    /// <exception cref="NotSupportedException">Throws NotSupportedException if objectType is not supported</exception>
    public static void SetUpTestObjects(IDatabase db, GarnetObjectType objectType, RedisKey[] keys, RedisValue[][] values)
    {
        ClassicAssert.AreEqual(keys.Length, values.Length);

        switch (objectType)
        {
            case GarnetObjectType.Set:
                for (var i = 0; i < keys.Length; i++)
                {
                    var added = db.SetAdd(keys[i], values[i]);
                    ClassicAssert.AreEqual(values[i].Select(v => v.ToString()).Distinct().Count(), added);
                }
                break;
            case GarnetObjectType.List:
                for (var i = 0; i < keys.Length; i++)
                {
                    var added = db.ListRightPush(keys[i], values[i]);
                    ClassicAssert.AreEqual(values[i].Length, added);
                }
                break;
            default:
                throw new NotSupportedException();
        }
    }

    /// <summary>
    /// Assert that calling testAction results in a wrong type error
    /// </summary>
    /// <param name="testAction">Action to test</param>
    public static void CheckCommandOnWrongTypeObjectSE(Action testAction)
    {
        var expectedError = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
        try
        {
            testAction();
            Assert.Fail();
        }
        catch (RedisServerException e)
        {
            ClassicAssert.AreEqual(expectedError, e.Message);
        }
        catch (AggregateException ae)
        {
            var rse = ae.InnerExceptions.FirstOrDefault(e => e is RedisServerException);
            ClassicAssert.IsNotNull(rse);
            ClassicAssert.AreEqual(expectedError, rse.Message);
        }
    }
}