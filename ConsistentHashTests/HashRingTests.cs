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
    public void FindNodeFor_WithIdenticalKey_ReturnsExactNode()
    {
        var hashRing = new HashRing();
        var nodeKey = "node1";
        hashRing.AddNode(nodeKey);
        hashRing.AddNode("node2");
        var result = hashRing.FindNodeFor(nodeKey);
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

}