using ConsistentHash;

namespace ConsistentHashTests;

public class HashRingTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void AddNode_WithInvalidKey_Throws(string invalidKey)
    {
        var hashRing = new HashRing();
        Assert.ThrowsAny<ArgumentException>(() => hashRing.AddNode(invalidKey));
    }

    [Fact]
    public void AddNode_WithValidKey_ReturnsTrue()
    {
        var hashRing = new HashRing();
        var nodeKey = "node1";

        var result = hashRing.AddNode(nodeKey);

        Assert.True(result);
    }

    [Fact]
    public void AddNode_WithDuplicateKey_ReturnsFalse()
    {
        var hashRing = new HashRing();
        var nodeKey = "node1";
        hashRing.AddNode(nodeKey);

        var result = hashRing.AddNode(nodeKey);

        Assert.False(result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void RemoveNode_WithInvalidKey_Throws(string invalidKey)
    {
        var hashRing = new HashRing();
        Assert.ThrowsAny<ArgumentException>(() => hashRing.RemoveNode(invalidKey));
    }

    [Fact]
    public void RemoveNode_WithExistingKey_ReturnsTrue()
    {
        var hashRing = new HashRing();
        var nodeKey = "node1";
        hashRing.AddNode(nodeKey);
        var result = hashRing.RemoveNode(nodeKey);
        Assert.True(result);
    }

    [Fact]
    public void RemoveNode_WithNonExistentKey_ReturnsFalse()
    {
        var hashRing = new HashRing();
        var result = hashRing.RemoveNode("nonexistent");
        Assert.False(result);
    }

    [Fact]
    public void RemoveNode_RemovesTheNode()
    {
        var hashRing = new HashRing();
        hashRing.AddNode("node1");
        hashRing.RemoveNode("node1");
        Assert.Throws<InvalidOperationException>(() => hashRing.FindNodeFor("node1"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void FindNodeFor_WithInvalidKey_Throws(string invalidKey)
    {
        var hashRing = new HashRing();
        hashRing.AddNode("node1");
        Assert.ThrowsAny<ArgumentException>(() => hashRing.FindNodeFor(invalidKey));
    }

    [Fact]
    public void FindNodeFor_WithEmptyRing_Throws()
    {
        var hashRing = new HashRing();
        Assert.Throws<InvalidOperationException>(() => hashRing.FindNodeFor("key1"));
    }

    [Fact]
    public void FindNodeFor_WithSingleNode_ReturnsNode()
    {
        var hashRing = new HashRing();
        var nodeKey = "node1";
        hashRing.AddNode(nodeKey);
        var result = hashRing.FindNodeFor("key1");
        Assert.Equal(nodeKey, result);
    }

    [Fact]
    public void FindNodeFor_WithMultipleNodes_ReturnsConsistentResults()
    {
        var hashRing = new HashRing();
        hashRing.AddNode("node3");
        hashRing.AddNode("node2");
        hashRing.AddNode("node1");

        var testKey = "testKey";
        var result1 = hashRing.FindNodeFor(testKey);
        var result2 = hashRing.FindNodeFor(testKey);

        Assert.Equal(result1, result2);
        Assert.Contains(result1, new[]
        {
            "node1", "node2", "node3"
        });
    }

    [Fact]
    public void FindNodeFor_AfterNodeRemoval_ANodeIsStillFound()
    {
        var hashRing = new HashRing();
        hashRing.AddNode("node1");
        hashRing.AddNode("node2");
        hashRing.AddNode("node3");

        var testKeys = new Dictionary<string, string>();

        // try a bunch of keys until we find one for each node
        for (var i = 0; i < 100 && testKeys.Count < 3; i++)
        {
            var node = hashRing.FindNodeFor($"key{i}");
            if (!testKeys.ContainsKey(node)) testKeys.Add(node, $"key{i}");
        }
        
        if (testKeys.Count < 3) Assert.Fail("unable to find keys for all nodes");
        
        hashRing.RemoveNode("node2");

        foreach (var key in testKeys.Keys)
        {
            var newNode = hashRing.FindNodeFor(key);
            Assert.True(newNode is "node1" or "node3");
        }
    }

    [Fact]
    public void Creating_Ring_With_Zero_VNode_Count_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => new HashRing(0));
    }

    /// <summary>
    /// Verifies that virtual nodes improve load distribution across nodes in a consistent hash ring.
    /// 
    /// Virtual nodes are a key feature of consistent hashing that helps solve the "hot spot" problem
    /// where keys might cluster around certain physical nodes, creating uneven load distribution.
    /// 
    /// This test demonstrates that:
    /// 1. With few virtual nodes (1 per physical node), keys may distribute unevenly
    /// 2. With many virtual nodes (100 per physical node), keys distribute more evenly
    /// 3. Better distribution is measured by lower variance in key counts per node
    /// 
    /// The test works by:
    /// - Creating two identical rings with different virtual node counts
    /// - Distributing 10,000 test keys across both rings
    /// - Calculating the variance in how many keys each physical node receives
    /// - Asserting that the multi-virtual-node ring has lower variance (better distribution)
    /// 
    /// A lower variance indicates that the load is more evenly distributed across all nodes,
    /// which is the primary benefit of using virtual nodes in consistent hashing.
    ///
    /// contributed by Claude Code
    /// </summary>
    [Fact]
    public void VirtualNodes_ImproveDistribution()
    {
        var singleVnodeRing = new HashRing(1);
        var multiVnodeRing = new HashRing(100);
        
        singleVnodeRing.AddNode("node1");
        singleVnodeRing.AddNode("node2");
        singleVnodeRing.AddNode("node3");
        
        multiVnodeRing.AddNode("node1");
        multiVnodeRing.AddNode("node2");
        multiVnodeRing.AddNode("node3");

        var nodeCounts1 = new Dictionary<string, int> { ["node1"] = 0, ["node2"] = 0, ["node3"] = 0 };
        var nodeCounts100 = new Dictionary<string, int> { ["node1"] = 0, ["node2"] = 0, ["node3"] = 0 };

        for (var i = 0; i < 10000; i++)
        {
            var key = $"key{i}";
            nodeCounts1[singleVnodeRing.FindNodeFor(key)]++;
            nodeCounts100[multiVnodeRing.FindNodeFor(key)]++;
        }

        var singleVnodeVariance = CalculateVariance(nodeCounts1.Values);
        var multiVnodeVariance = CalculateVariance(nodeCounts100.Values);
        
        Assert.True(multiVnodeVariance < singleVnodeVariance, 
            $"Multi-vnode variance ({multiVnodeVariance:F2}) should be less than single-vnode variance ({singleVnodeVariance:F2})");
    }

    private static double CalculateVariance(IEnumerable<int> values)
    {
        var valueList = values.ToList();
        var mean = valueList.Average();
        return valueList.Select(v => Math.Pow(v - mean, 2)).Average();
    }

}