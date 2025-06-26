using System.Security.Cryptography;
using System.Text;

namespace ConsistentHash;

public class HashRing
{
    private readonly Dictionary<uint, string> _vnodeToNodeMap = new();
    private readonly Dictionary<string, List<uint>> _nodeToVnodeMap = new();
    private readonly List<uint> _vnodeList = new();
    private readonly ReaderWriterLockSlim _lock = new();
    private readonly uint _virtualNodeCount;

    public HashRing(uint virtualNodeCount = 1)
    {
        if (virtualNodeCount == 0) throw new ArgumentException("There must be at least one virtual node",  nameof(virtualNodeCount));
        _virtualNodeCount = virtualNodeCount;
    }

    /// <summary>
    /// Adds a new node to the hash ring at a position determined by hashing the node key.
    /// </summary>
    /// <param name="key">
    /// The unique identifier for the node to add. Must be a non-null, non-empty string 
    /// that doesn't consist only of whitespace characters.
    /// </param>
    /// <returns>
    /// <c>true</c> if the node was successfully added to the ring; 
    /// <c>false</c> if the node has already been added to the ring.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="key"/> is <c>null</c>, empty, or consists only of whitespace.
    /// </exception>
    /// <remarks>
    /// This method is thread-safe
    /// </remarks>
    public bool AddNode(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));

        _lock.EnterWriteLock();
        try
        {
            if (_nodeToVnodeMap.ContainsKey(key)) return false;
            
            List<uint> vnodes = [];
            for (var i = 0; i < _virtualNodeCount; i++)
            {
                var vnode = this.Hash($"{key}:{i}");
                var index = _vnodeList.BinarySearch(vnode);

                /* From the BinarySearch docs...
                 * [if the index is negative it] contains the bitwise complement
                 * of the index of the next element that is larger than item or,
                 * if there is no larger element, the bitwise complement of Count.
                 */
                index = ~index;
                vnodes.Add(vnode);
                _vnodeList.Insert(index, vnode); // add the vnode to our ring
                _vnodeToNodeMap.Add(vnode, key); // map the vnode to the key
            }
            _nodeToVnodeMap.Add(key, vnodes);            
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        return true;
    }

    /// <summary>
    /// Removes a node from the hash ring by its key.
    /// </summary>
    /// <param name="key">
    /// The unique identifier of the node to remove. Must be a non-null, non-empty string 
    /// that doesn't consist only of whitespace characters.
    /// </param>
    /// <returns>
    /// <c>true</c> if the key was found and successfully removed from the ring; 
    /// <c>false</c> if given key is not in the ring.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="key"/> is <c>null</c>, empty, or consists only of whitespace.
    /// </exception>
    /// <remarks>
    /// This method is thread-safe.
    /// </remarks>
    public bool RemoveNode(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        if (!_nodeToVnodeMap.TryGetValue(key, out var vnodes))  return false;
        
        _lock.EnterWriteLock();
        try
        {
            foreach (var vnode in vnodes)
            {
                var index = _vnodeList.BinarySearch(vnode);
                _vnodeList.RemoveAt(index);
                _vnodeToNodeMap.Remove(vnode);
            }
            _nodeToVnodeMap.Remove(key);
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        return true;
    }

    /// <summary>
    /// Finds the appropriate node in the hash ring for the given key using consistent hashing.
    /// </summary>
    /// <param name="key">
    /// The key to find a node for. Must be a non-null, non-empty string 
    /// that doesn't consist only of whitespace characters.
    /// </param>
    /// <returns>
    /// The key of the node that should handle the given key according to the consistent 
    /// hashing algorithm. This will be the first node in the ring whose hash value is 
    /// greater than or equal to the hash of the input key, or the first node in the ring 
    /// if no such node exists (wrap-around behavior).
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="key"/> is <c>null</c>, empty, or consists only of whitespace.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the hash ring is empty (contains no nodes). At least one node must be 
    /// added to the ring before this method can be called.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method is thread-safe.
    /// </para>
    /// </remarks>
    public string? FindNodeFor(string key)
    {
        var nodes = FindNodeFor(key, 1);
        return nodes?.Length > 0 ? nodes[0] : null;
    }

    /// <summary>
    /// Finds the appropriate node in the hash ring for the given key using consistent hashing.
    /// </summary>
    /// <param name="key">
    /// The key to find a node for. Must be a non-null, non-empty string 
    /// that doesn't consist only of whitespace characters.
    /// </param>
    /// <param name="replicationFactor">
    /// The number of nodes that this key should be replication over
    /// </param>
    /// <returns>
    /// The key of the node that should handle the given key according to the consistent 
    /// hashing algorithm. This will be the first node in the ring whose hash value is 
    /// greater than or equal to the hash of the input key, or the first node in the ring 
    /// if no such node exists (wrap-around behavior).
    ///
    /// More than one unique node will be returned if the replicationFactor is set to a value
    /// greater than one (1). The order of the returned nodes will match the sequence of the
    /// nodes on the ring starting with the first matched node. 
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="key"/> is <c>null</c>, empty, or consists only of whitespace.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the hash ring is empty (contains no nodes). At least one node must be 
    /// added to the ring before this method can be called.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This method is thread-safe.
    /// </para>
    /// </remarks>
    public string[] FindNodeFor(string key, uint replicationFactor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        if (_vnodeList.Count == 0)
            throw new InvalidOperationException("The list of nodes is empty. Add one or more nodes");

        if (replicationFactor > _nodeToVnodeMap.Count)
            throw new ArgumentOutOfRangeException(nameof(replicationFactor),
                "The replication factor cannot be larger than the number of registered nodes");
        
        var hash = this.Hash(key);
        _lock.EnterReadLock();
        try
        {
            /* From the BinarySearch docs...
             * [if the index is negative it] contains the bitwise complement
             * of the index of the next element that is larger than item or,
             * if there is no larger element, the bitwise complement of Count.
             */
            var index = _vnodeList.BinarySearch(hash);
            if (index < 0) index = ~index;
            
            List<string> nodes = [];
            var resetCount = 0;
            while (nodes.Count < replicationFactor)
            {
                if (index >= _vnodeList.Count)
                {
                    index = 0;
                    resetCount++;
                    
                    // we somehow made it around the ring at least once but didn't 
                    // collect enough unique nodes to make the replication factor.
                    // return what we have.
                    if (resetCount >= 2) return nodes.ToArray();
                }
                var vnode = _vnodeList[index];
                if (!_vnodeToNodeMap.TryGetValue(vnode, out var node))
                    throw new InvalidOperationException($"A matching vnode was found but could not be mapped to the node");
                if (!nodes.Contains(node)) nodes.Add(node);
                index++;
            }

            return nodes.ToArray();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private uint Hash(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return BitConverter.ToUInt32(bytes, 0);
    }
}