// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.server.Vector.Filter;
using Garnet.server.Vector.QueryEngine;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for the Vector Query Engine.
    /// Tests attribute indexes, filter bitmaps, selectivity estimation,
    /// query planning, and end-to-end query engine flow.
    /// </summary>
    [TestFixture]
    public class VectorQueryEngineTests
    {
        #region HashIndex Tests

        [Test]
        public void HashIndex_AddAndLookup()
        {
            var index = new HashIndex("category");

            index.Add(1, "electronics");
            index.Add(2, "electronics");
            index.Add(3, "clothing");
            index.Add(4, "electronics");

            var result = index.GetEqual("electronics");
            ClassicAssert.AreEqual(3, result.Count);
            ClassicAssert.IsTrue(result.Contains(1));
            ClassicAssert.IsTrue(result.Contains(2));
            ClassicAssert.IsTrue(result.Contains(4));

            var clothing = index.GetEqual("clothing");
            ClassicAssert.AreEqual(1, clothing.Count);
            ClassicAssert.IsTrue(clothing.Contains(3));
        }

        [Test]
        public void HashIndex_CaseInsensitiveLookup()
        {
            var index = new HashIndex("status");

            index.Add(1, "Active");
            index.Add(2, "active");

            var result = index.GetEqual("ACTIVE");
            ClassicAssert.AreEqual(2, result.Count);
        }

        [Test]
        public void HashIndex_Remove()
        {
            var index = new HashIndex("category");

            index.Add(1, "electronics");
            index.Add(2, "electronics");
            index.Remove(1, "electronics");

            var result = index.GetEqual("electronics");
            ClassicAssert.AreEqual(1, result.Count);
            ClassicAssert.IsTrue(result.Contains(2));
        }

        [Test]
        public void HashIndex_RemoveLastEntry()
        {
            var index = new HashIndex("category");

            index.Add(1, "electronics");
            index.Remove(1, "electronics");

            var result = index.GetEqual("electronics");
            ClassicAssert.AreEqual(0, result.Count);
            ClassicAssert.AreEqual(0, index.Statistics.TotalEntries);
            ClassicAssert.AreEqual(0, index.Statistics.DistinctValues);
        }

        [Test]
        public void HashIndex_NotFound()
        {
            var index = new HashIndex("category");
            index.Add(1, "electronics");

            var result = index.GetEqual("nonexistent");
            ClassicAssert.AreEqual(0, result.Count);
        }

        [Test]
        public void HashIndex_Statistics()
        {
            var index = new HashIndex("category");

            index.Add(1, "electronics");
            index.Add(2, "clothing");
            index.Add(3, "electronics");

            ClassicAssert.AreEqual(3, index.Statistics.TotalEntries);
            ClassicAssert.AreEqual(2, index.Statistics.DistinctValues);
        }

        [Test]
        public void HashIndex_SelectivityEstimation()
        {
            var index = new HashIndex("category");

            // 1000 items across 10 categories (roughly 100 each)
            for (int i = 0; i < 1000; i++)
            {
                index.Add(i, $"category_{i % 10}");
            }

            // Uniform distribution: selectivity ≈ 1/10
            var selectivity = index.EstimateEqualitySelectivity();
            ClassicAssert.AreEqual(0.1, selectivity, 0.01);

            // Specific value: 100 out of 1000
            var specificSel = index.EstimateEqualitySelectivity("category_0");
            ClassicAssert.AreEqual(0.1, specificSel, 0.01);
        }

        [Test]
        public void HashIndex_Clear()
        {
            var index = new HashIndex("category");
            index.Add(1, "electronics");
            index.Add(2, "clothing");

            index.Clear();

            ClassicAssert.AreEqual(0, index.Statistics.TotalEntries);
            ClassicAssert.AreEqual(0, index.GetEqual("electronics").Count);
        }

        [Test]
        public void HashIndex_DuplicateAdd()
        {
            var index = new HashIndex("category");

            index.Add(1, "electronics");
            index.Add(1, "electronics"); // Duplicate - should not increase count

            ClassicAssert.AreEqual(1, index.Statistics.TotalEntries);
            ClassicAssert.AreEqual(1, index.GetEqual("electronics").Count);
        }

        #endregion

        #region RangeIndex Tests

        [Test]
        public void RangeIndex_AddAndRangeQuery()
        {
            var index = new RangeIndex("price");

            index.Add(1, 10.0);
            index.Add(2, 50.0);
            index.Add(3, 100.0);
            index.Add(4, 200.0);
            index.Add(5, 500.0);

            var result = index.GetRange(50.0, 200.0, minInclusive: true, maxInclusive: true);
            ClassicAssert.AreEqual(3, result.Count);
            ClassicAssert.IsTrue(result.Contains(2)); // 50
            ClassicAssert.IsTrue(result.Contains(3)); // 100
            ClassicAssert.IsTrue(result.Contains(4)); // 200
        }

        [Test]
        public void RangeIndex_GreaterThan()
        {
            var index = new RangeIndex("price");

            index.Add(1, 10.0);
            index.Add(2, 50.0);
            index.Add(3, 100.0);

            var exclusive = index.GetGreaterThan(50.0, inclusive: false);
            ClassicAssert.AreEqual(1, exclusive.Count);
            ClassicAssert.IsTrue(exclusive.Contains(3));

            var inclusive = index.GetGreaterThan(50.0, inclusive: true);
            ClassicAssert.AreEqual(2, inclusive.Count);
        }

        [Test]
        public void RangeIndex_LessThan()
        {
            var index = new RangeIndex("price");

            index.Add(1, 10.0);
            index.Add(2, 50.0);
            index.Add(3, 100.0);

            var exclusive = index.GetLessThan(50.0, inclusive: false);
            ClassicAssert.AreEqual(1, exclusive.Count);
            ClassicAssert.IsTrue(exclusive.Contains(1));

            var inclusive = index.GetLessThan(50.0, inclusive: true);
            ClassicAssert.AreEqual(2, inclusive.Count);
        }

        [Test]
        public void RangeIndex_Statistics()
        {
            var index = new RangeIndex("price");

            index.Add(1, 10.0);
            index.Add(2, 50.0);
            index.Add(3, 100.0);

            ClassicAssert.AreEqual(3, index.Statistics.TotalEntries);
            ClassicAssert.AreEqual(3, index.Statistics.DistinctValues);
            ClassicAssert.AreEqual(10.0, index.Statistics.MinValue);
            ClassicAssert.AreEqual(100.0, index.Statistics.MaxValue);
        }

        [Test]
        public void RangeIndex_Remove()
        {
            var index = new RangeIndex("price");

            index.Add(1, 10.0);
            index.Add(2, 50.0);
            index.Remove(1, 10.0);

            ClassicAssert.AreEqual(1, index.Statistics.TotalEntries);
            ClassicAssert.AreEqual(50.0, index.Statistics.MinValue);
        }

        [Test]
        public void RangeIndex_EqualityLookup()
        {
            var index = new RangeIndex("year");

            index.Add(1, 2020.0);
            index.Add(2, 2020.0);
            index.Add(3, 2021.0);

            var result = index.GetEqual(2020.0);
            ClassicAssert.AreEqual(2, result.Count);
        }

        [Test]
        public void RangeIndex_SelectivityEstimation()
        {
            var index = new RangeIndex("price");

            // Prices from 0 to 100
            for (int i = 0; i <= 100; i++)
            {
                index.Add(i, (double)i);
            }

            // Range 0-50 should be about 50% selectivity
            var sel = index.EstimateRangeSelectivity(0, 50);
            ClassicAssert.AreEqual(0.5, sel, 0.05);

            // Range 75-100 should be about 25%
            sel = index.EstimateRangeSelectivity(75, 100);
            ClassicAssert.AreEqual(0.25, sel, 0.05);
        }

        #endregion

        #region AttributeIndexManager Tests

        [Test]
        public void IndexManager_CreateAndGetIndex()
        {
            using var manager = new AttributeIndexManager("testset");

            ClassicAssert.IsTrue(manager.CreateIndex("category", AttributeIndexType.Hash));
            ClassicAssert.IsTrue(manager.CreateIndex("price", AttributeIndexType.Range));

            var catIndex = manager.GetIndex("category");
            ClassicAssert.IsNotNull(catIndex);
            ClassicAssert.AreEqual(AttributeIndexType.Hash, catIndex.IndexType);

            var priceIndex = manager.GetIndex("price");
            ClassicAssert.IsNotNull(priceIndex);
            ClassicAssert.AreEqual(AttributeIndexType.Range, priceIndex.IndexType);
        }

        [Test]
        public void IndexManager_DuplicateCreate()
        {
            using var manager = new AttributeIndexManager("testset");

            ClassicAssert.IsTrue(manager.CreateIndex("category", AttributeIndexType.Hash));
            ClassicAssert.IsFalse(manager.CreateIndex("category", AttributeIndexType.Hash));
        }

        [Test]
        public void IndexManager_Drop()
        {
            using var manager = new AttributeIndexManager("testset");

            manager.CreateIndex("category", AttributeIndexType.Hash);
            ClassicAssert.IsTrue(manager.DropIndex("category"));
            ClassicAssert.IsNull(manager.GetIndex("category"));
            ClassicAssert.IsFalse(manager.DropIndex("category")); // Already dropped
        }

        [Test]
        public void IndexManager_OnVectorAdded()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);
            manager.CreateIndex("price", AttributeIndexType.Range);

            var json = System.Text.Encoding.UTF8.GetBytes("{\"category\":\"electronics\",\"price\":99.99}");
            manager.OnVectorAdded(1, json);

            var catIndex = manager.GetIndex("category");
            var result = catIndex.GetEqual("electronics");
            ClassicAssert.AreEqual(1, result.Count);

            var priceIndex = manager.GetIndex("price") as RangeIndex;
            ClassicAssert.IsNotNull(priceIndex);
            ClassicAssert.AreEqual(99.99, priceIndex.Statistics.MinValue);
        }

        [Test]
        public void IndexManager_OnVectorRemoved()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            var json = System.Text.Encoding.UTF8.GetBytes("{\"category\":\"electronics\"}");
            manager.OnVectorAdded(1, json);
            manager.OnVectorRemoved(1, json);

            var result = manager.GetIndex("category").GetEqual("electronics");
            ClassicAssert.AreEqual(0, result.Count);
        }

        [Test]
        public void IndexManager_GetIndexesForFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);
            manager.CreateIndex("price", AttributeIndexType.Range);

            // Filter: .category == 'electronics'
            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "electronics" }
            };

            var indexes = manager.GetIndexesForFilter(filter);
            ClassicAssert.AreEqual(1, indexes.Count);
            ClassicAssert.AreEqual("category", indexes[0].FieldName);
        }

        [Test]
        public void IndexManager_GetIndexesForComplexFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);
            manager.CreateIndex("price", AttributeIndexType.Range);

            // Filter: .category == 'electronics' AND .price > 100
            var filter = new BinaryExpr
            {
                Left = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".category" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "electronics" }
                },
                Operator = "and",
                Right = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".price" },
                    Operator = ">",
                    Right = new LiteralExpr { Value = 100.0 }
                }
            };

            var indexes = manager.GetIndexesForFilter(filter);
            ClassicAssert.AreEqual(2, indexes.Count);
        }

        #endregion

        #region FilterBitmap Tests

        [Test]
        public void Bitmap_SetAndCheck()
        {
            var bitmap = new FilterBitmap(1000);

            bitmap.Set(0);
            bitmap.Set(42);
            bitmap.Set(999);

            ClassicAssert.IsTrue(bitmap.IsSet(0));
            ClassicAssert.IsTrue(bitmap.IsSet(42));
            ClassicAssert.IsTrue(bitmap.IsSet(999));
            ClassicAssert.IsFalse(bitmap.IsSet(1));
            ClassicAssert.IsFalse(bitmap.IsSet(500));
            ClassicAssert.AreEqual(3, bitmap.PopCount);
        }

        [Test]
        public void Bitmap_Clear()
        {
            var bitmap = new FilterBitmap(100);

            bitmap.Set(50);
            ClassicAssert.IsTrue(bitmap.IsSet(50));

            bitmap.Clear(50);
            ClassicAssert.IsFalse(bitmap.IsSet(50));
            ClassicAssert.AreEqual(0, bitmap.PopCount);
        }

        [Test]
        public void Bitmap_And()
        {
            var a = new FilterBitmap(100);
            var b = new FilterBitmap(100);

            a.Set(1); a.Set(2); a.Set(3);
            b.Set(2); b.Set(3); b.Set(4);

            var result = FilterBitmap.And(a, b);
            ClassicAssert.IsFalse(result.IsSet(1));
            ClassicAssert.IsTrue(result.IsSet(2));
            ClassicAssert.IsTrue(result.IsSet(3));
            ClassicAssert.IsFalse(result.IsSet(4));
            ClassicAssert.AreEqual(2, result.PopCount);
        }

        [Test]
        public void Bitmap_Or()
        {
            var a = new FilterBitmap(100);
            var b = new FilterBitmap(100);

            a.Set(1); a.Set(2);
            b.Set(3); b.Set(4);

            var result = FilterBitmap.Or(a, b);
            ClassicAssert.IsTrue(result.IsSet(1));
            ClassicAssert.IsTrue(result.IsSet(2));
            ClassicAssert.IsTrue(result.IsSet(3));
            ClassicAssert.IsTrue(result.IsSet(4));
            ClassicAssert.AreEqual(4, result.PopCount);
        }

        [Test]
        public void Bitmap_Not()
        {
            var a = new FilterBitmap(8);
            a.Set(0);
            a.Set(2);
            a.Set(4);

            var result = FilterBitmap.Not(a, 8);
            ClassicAssert.IsFalse(result.IsSet(0));
            ClassicAssert.IsTrue(result.IsSet(1));
            ClassicAssert.IsFalse(result.IsSet(2));
            ClassicAssert.IsTrue(result.IsSet(3));
            ClassicAssert.IsFalse(result.IsSet(4));
            ClassicAssert.IsTrue(result.IsSet(5));
            ClassicAssert.AreEqual(5, result.PopCount);
        }

        [Test]
        public void Bitmap_FromIds()
        {
            var ids = new HashSet<long> { 10, 20, 30 };
            var bitmap = FilterBitmap.FromIds(ids, 100);

            ClassicAssert.IsTrue(bitmap.IsSet(10));
            ClassicAssert.IsTrue(bitmap.IsSet(20));
            ClassicAssert.IsTrue(bitmap.IsSet(30));
            ClassicAssert.IsFalse(bitmap.IsSet(15));
            ClassicAssert.AreEqual(3, bitmap.PopCount);
        }

        [Test]
        public void Bitmap_EnumerateSetBits()
        {
            var bitmap = new FilterBitmap(200);
            bitmap.Set(5);
            bitmap.Set(64);
            bitmap.Set(128);
            bitmap.Set(199);

            var bits = bitmap.EnumerateSetBits().ToList();
            ClassicAssert.AreEqual(4, bits.Count);
            CollectionAssert.AreEqual((long[])[5, 64, 128, 199], bits);
        }

        [Test]
        public void Bitmap_Selectivity()
        {
            var bitmap = new FilterBitmap(1000);
            for (int i = 0; i < 100; i++)
            {
                bitmap.Set(i);
            }

            ClassicAssert.AreEqual(0.1, bitmap.Selectivity, 0.001);
        }

        #endregion

        #region SelectivityEstimator Tests

        [Test]
        public void Estimator_NoFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            var estimator = new SelectivityEstimator(manager);

            ClassicAssert.AreEqual(1.0, estimator.Estimate(null));
        }

        [Test]
        public void Estimator_EqualityWithIndex()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            // Add data: 10 categories, 100 items each
            for (int i = 0; i < 1000; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"cat_{i % 10}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var estimator = new SelectivityEstimator(manager);

            // .category == 'cat_0' → expect ~0.1
            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "cat_0" }
            };

            var selectivity = estimator.Estimate(filter);
            ClassicAssert.AreEqual(0.1, selectivity, 0.02);
        }

        [Test]
        public void Estimator_AndCombination()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);
            manager.CreateIndex("status", AttributeIndexType.Hash);

            for (int i = 0; i < 1000; i++)
            {
                var cat = $"cat_{i % 10}";
                var status = i % 2 == 0 ? "active" : "inactive";
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"{cat}\",\"status\":\"{status}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var estimator = new SelectivityEstimator(manager);

            // .category == 'cat_0' AND .status == 'active'
            // Expected: 0.1 * 0.5 = 0.05
            var filter = new BinaryExpr
            {
                Left = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".category" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "cat_0" }
                },
                Operator = "and",
                Right = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".status" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "active" }
                }
            };

            var selectivity = estimator.Estimate(filter);
            ClassicAssert.AreEqual(0.05, selectivity, 0.02);
        }

        [Test]
        public void Estimator_OrCombination()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            for (int i = 0; i < 1000; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"cat_{i % 10}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var estimator = new SelectivityEstimator(manager);

            // .category == 'cat_0' OR .category == 'cat_1'
            // Expected: 0.1 + 0.1 - 0.01 = 0.19
            var filter = new BinaryExpr
            {
                Left = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".category" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "cat_0" }
                },
                Operator = "or",
                Right = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".category" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "cat_1" }
                }
            };

            var selectivity = estimator.Estimate(filter);
            ClassicAssert.AreEqual(0.19, selectivity, 0.03);
        }

        [Test]
        public void Estimator_RangeWithIndex()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("price", AttributeIndexType.Range);

            for (int i = 0; i < 100; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"price\":{i}}}");
                manager.OnVectorAdded(i, json);
            }

            var estimator = new SelectivityEstimator(manager);

            // .price > 75 → expect ~25% selectivity
            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".price" },
                Operator = ">",
                Right = new LiteralExpr { Value = 75.0 }
            };

            var selectivity = estimator.Estimate(filter);
            // (99 - 75) / (99 - 0) ≈ 0.24
            ClassicAssert.IsTrue(selectivity > 0.15 && selectivity < 0.35,
                $"Expected selectivity ~0.24, got {selectivity}");
        }

        [Test]
        public void Estimator_NoIndex_DefaultSelectivity()
        {
            using var manager = new AttributeIndexManager("testset");
            var estimator = new SelectivityEstimator(manager);

            // No index exists for 'category'
            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "electronics" }
            };

            var selectivity = estimator.Estimate(filter);
            ClassicAssert.AreEqual(0.5, selectivity); // Default when no index
        }

        #endregion

        #region VectorQueryPlanner Tests

        [Test]
        public void Planner_NoFilter_PostFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            var planner = new VectorQueryPlanner(manager);

            var plan = planner.BuildPlan(null, null, 10, 1000);

            ClassicAssert.AreEqual(ExecutionStrategy.PostFilter, plan.Strategy);
            ClassicAssert.AreEqual(1.0, plan.EstimatedSelectivity);
        }

        [Test]
        public void Planner_HighSelectivity_PostFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("active", AttributeIndexType.Hash);

            // 90% are active
            for (int i = 0; i < 1000; i++)
            {
                var val = i < 900 ? "true" : "false";
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"active\":\"{val}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var planner = new VectorQueryPlanner(manager);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".active" },
                Operator = "==",
                Right = new LiteralExpr { Value = "true" }
            };

            var plan = planner.BuildPlan(filter, ".active == 'true'", 10, 1000);

            // 90% selectivity → PostFilter
            ClassicAssert.AreEqual(ExecutionStrategy.PostFilter, plan.Strategy);
        }

        [Test]
        public void Planner_LowSelectivity_PreFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("sku", AttributeIndexType.Hash);

            // Each SKU is unique among 100,000 items → 0.001% selectivity
            for (int i = 0; i < 100000; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"sku\":\"SKU_{i}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var planner = new VectorQueryPlanner(manager);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".sku" },
                Operator = "==",
                Right = new LiteralExpr { Value = "SKU_42" }
            };

            var plan = planner.BuildPlan(filter, ".sku == 'SKU_42'", 10, 100000);

            // 0.001% selectivity → PreFilter
            ClassicAssert.AreEqual(ExecutionStrategy.PreFilter, plan.Strategy);
        }

        [Test]
        public void Planner_MediumSelectivity_FilterAware()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            // 100 categories among 10,000 items → 1% selectivity per category
            for (int i = 0; i < 10000; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"cat_{i % 100}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var planner = new VectorQueryPlanner(manager);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "cat_0" }
            };

            var plan = planner.BuildPlan(filter, ".category == 'cat_0'", 10, 10000);

            // 1% selectivity → FilterAware
            ClassicAssert.AreEqual(ExecutionStrategy.FilterAware, plan.Strategy);
        }

        [Test]
        public void Planner_NoIndex_AlwaysPostFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            // No indexes created
            var planner = new VectorQueryPlanner(manager);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "electronics" }
            };

            var plan = planner.BuildPlan(filter, ".category == 'electronics'", 10, 1000);

            // No index → must post-filter
            ClassicAssert.AreEqual(ExecutionStrategy.PostFilter, plan.Strategy);
        }

        [Test]
        public void Planner_PlanContainsIndexNames()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            for (int i = 0; i < 100; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"cat_{i}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var planner = new VectorQueryPlanner(manager);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "cat_0" }
            };

            var plan = planner.BuildPlan(filter, ".category == 'cat_0'", 10, 100);

            ClassicAssert.IsTrue(plan.IndexNames.Contains("category"));
        }

        #endregion

        #region FilterBitmapBuilder Tests

        [Test]
        public void BitmapBuilder_SimpleEquality()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            for (int i = 0; i < 100; i++)
            {
                var cat = i < 10 ? "electronics" : "other";
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"{cat}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var builder = new FilterBitmapBuilder(manager, 100);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "electronics" }
            };

            var bitmap = builder.Build(filter);

            ClassicAssert.AreEqual(10, bitmap.PopCount);
            for (int i = 0; i < 10; i++)
            {
                ClassicAssert.IsTrue(bitmap.IsSet(i));
            }
            ClassicAssert.IsFalse(builder.HasUnresolvedPredicates);
        }

        [Test]
        public void BitmapBuilder_AndCombination()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);
            manager.CreateIndex("status", AttributeIndexType.Hash);

            for (int i = 0; i < 100; i++)
            {
                var cat = i % 2 == 0 ? "electronics" : "clothing";
                var status = i < 50 ? "active" : "inactive";
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"{cat}\",\"status\":\"{status}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var builder = new FilterBitmapBuilder(manager, 100);

            // .category == 'electronics' AND .status == 'active'
            var filter = new BinaryExpr
            {
                Left = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".category" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "electronics" }
                },
                Operator = "and",
                Right = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".status" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "active" }
                }
            };

            var bitmap = builder.Build(filter);

            // Even IDs (0,2,4,...,48) that are also < 50 → 25 items
            ClassicAssert.AreEqual(25, bitmap.PopCount);
            ClassicAssert.IsFalse(builder.HasUnresolvedPredicates);
        }

        [Test]
        public void BitmapBuilder_UnresolvedPredicate()
        {
            using var manager = new AttributeIndexManager("testset");
            // No index for 'category'

            var builder = new FilterBitmapBuilder(manager, 100);

            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".category" },
                Operator = "==",
                Right = new LiteralExpr { Value = "electronics" }
            };

            var bitmap = builder.Build(filter);

            // All bits set (fallback to post-filter)
            ClassicAssert.AreEqual(100, bitmap.PopCount);
            ClassicAssert.IsTrue(builder.HasUnresolvedPredicates);
        }

        [Test]
        public void BitmapBuilder_NotOperator()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            for (int i = 0; i < 10; i++)
            {
                var cat = i < 3 ? "electronics" : "other";
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"{cat}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var builder = new FilterBitmapBuilder(manager, 10);

            // NOT (.category == 'electronics')
            var filter = new UnaryExpr
            {
                Operator = "not",
                Operand = new BinaryExpr
                {
                    Left = new MemberExpr { Property = ".category" },
                    Operator = "==",
                    Right = new LiteralExpr { Value = "electronics" }
                }
            };

            var bitmap = builder.Build(filter);
            ClassicAssert.AreEqual(7, bitmap.PopCount);
        }

        [Test]
        public void BitmapBuilder_RangeQuery()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("price", AttributeIndexType.Range);

            for (int i = 0; i < 100; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"price\":{i}}}");
                manager.OnVectorAdded(i, json);
            }

            var builder = new FilterBitmapBuilder(manager, 100);

            // .price > 90
            var filter = new BinaryExpr
            {
                Left = new MemberExpr { Property = ".price" },
                Operator = ">",
                Right = new LiteralExpr { Value = 90.0 }
            };

            var bitmap = builder.Build(filter);

            // IDs 91-99 → 9 items
            ClassicAssert.AreEqual(9, bitmap.PopCount);
            ClassicAssert.IsFalse(bitmap.IsSet(90));
            ClassicAssert.IsTrue(bitmap.IsSet(91));
            ClassicAssert.IsTrue(bitmap.IsSet(99));
        }

        #endregion

        #region VectorQueryEngine Tests

        [Test]
        public void QueryEngine_PlanNoFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            var engine = new VectorQueryEngine(manager);

            var plan = engine.Plan([], 10, 1000);

            ClassicAssert.AreEqual(ExecutionStrategy.PostFilter, plan.Strategy);
            ClassicAssert.IsNull(plan.Filter);
        }

        [Test]
        public void QueryEngine_PlanWithFilter()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            for (int i = 0; i < 1000; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"cat_{i % 100}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var engine = new VectorQueryEngine(manager);
            var filterBytes = System.Text.Encoding.UTF8.GetBytes(".category == 'cat_0'");

            var plan = engine.Plan(filterBytes, 10, 1000);

            ClassicAssert.IsNotNull(plan.Filter);
            ClassicAssert.IsNotNull(plan.FilterString);
            ClassicAssert.IsTrue(plan.EstimatedSelectivity > 0);
            ClassicAssert.IsTrue(plan.EstimatedSelectivity < 1.0);
        }

        [Test]
        public void QueryEngine_ExpressionCaching()
        {
            using var manager = new AttributeIndexManager("testset");
            var engine = new VectorQueryEngine(manager);

            var filterBytes = System.Text.Encoding.UTF8.GetBytes(".category == 'electronics'");

            // Plan twice with same filter
            var plan1 = engine.Plan(filterBytes, 10, 1000);
            var plan2 = engine.Plan(filterBytes, 10, 1000);

            // Both should parse successfully (cached)
            ClassicAssert.IsNotNull(plan1.Filter);
            ClassicAssert.IsNotNull(plan2.Filter);
        }

        [Test]
        public void QueryEngine_BuildBitmap()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("category", AttributeIndexType.Hash);

            for (int i = 0; i < 100; i++)
            {
                var cat = i < 10 ? "electronics" : "other";
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"category\":\"{cat}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var engine = new VectorQueryEngine(manager);
            var filterBytes = System.Text.Encoding.UTF8.GetBytes(".category == 'electronics'");
            var plan = engine.Plan(filterBytes, 10, 100);

            var (bitmap, needsPostFilter) = engine.BuildFilterBitmap(plan, 100);

            ClassicAssert.IsNotNull(bitmap);
            ClassicAssert.AreEqual(10, bitmap.PopCount);
            ClassicAssert.IsFalse(needsPostFilter);
        }

        [Test]
        public void QueryEngine_GetPreFilterCandidates()
        {
            using var manager = new AttributeIndexManager("testset");
            manager.CreateIndex("sku", AttributeIndexType.Hash);

            for (int i = 0; i < 1000; i++)
            {
                var json = System.Text.Encoding.UTF8.GetBytes($"{{\"sku\":\"SKU_{i}\"}}");
                manager.OnVectorAdded(i, json);
            }

            var engine = new VectorQueryEngine(manager);
            var filterBytes = System.Text.Encoding.UTF8.GetBytes(".sku == 'SKU_42'");
            var plan = engine.Plan(filterBytes, 10, 1000);

            var candidates = engine.GetPreFilterCandidates(plan, 1000);

            ClassicAssert.AreEqual(1, candidates.Count);
            ClassicAssert.IsTrue(candidates.Contains(42));
        }

        #endregion

        #region ExecutionCost Tests

        [Test]
        public void ExecutionCost_PostFilterHigherThanPreFilter_LowSelectivity()
        {
            // For very low selectivity, pre-filter should be cheaper
            var postFilterCost = new ExecutionCost
            {
                IndexLookups = 0,
                DistanceComputations = 10000, // Need to search many candidates
                IOOperations = 10000,
                FilterEvaluations = 10000,
            };

            var preFilterCost = new ExecutionCost
            {
                IndexLookups = 1,
                DistanceComputations = 10, // Only compute distances for matching candidates
                IOOperations = 10,
                FilterEvaluations = 0,
            };

            ClassicAssert.IsTrue(preFilterCost.TotalCost < postFilterCost.TotalCost,
                $"PreFilter cost ({preFilterCost.TotalCost}) should be less than PostFilter ({postFilterCost.TotalCost})");
        }

        [Test]
        public void ExecutionCost_PostFilterCheaper_HighSelectivity()
        {
            // For high selectivity, post-filter should be cheaper (no index overhead)
            var postFilterCost = new ExecutionCost
            {
                IndexLookups = 0,
                DistanceComputations = 11, // Most candidates match
                IOOperations = 11,
                FilterEvaluations = 11,
            };

            var preFilterCost = new ExecutionCost
            {
                IndexLookups = 1,
                DistanceComputations = 900, // Loading 90% of dataset
                IOOperations = 900,
                FilterEvaluations = 0,
            };

            ClassicAssert.IsTrue(postFilterCost.TotalCost < preFilterCost.TotalCost,
                $"PostFilter cost ({postFilterCost.TotalCost}) should be less than PreFilter ({preFilterCost.TotalCost})");
        }

        #endregion

        #region QueryPlan Tests

        [Test]
        public void QueryPlan_ToString()
        {
            var plan = new QueryPlan
            {
                Strategy = ExecutionStrategy.FilterAware,
                EstimatedSelectivity = 0.05,
                EstimatedCandidates = 500,
                EstimatedCost = new ExecutionCost
                {
                    IndexLookups = 1,
                    DistanceComputations = 15,
                    IOOperations = 20,
                    FilterEvaluations = 0,
                },
                IndexNames = ["category"],
                FilterString = ".category == 'electronics'",
            };

            var str = plan.ToString();
            ClassicAssert.IsTrue(str.Contains("FilterAware"));
            ClassicAssert.IsTrue(str.Contains("500"));
        }

        #endregion
    }
}
