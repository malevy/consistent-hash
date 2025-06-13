using System.Security.Cryptography;
using System.Text;

namespace ConsistentHash;

public class HashRing
{
    private readonly List<int> _hashList = new();
    private readonly List<string> _nodeList = new();
    private readonly ReaderWriterLockSlim _lock = new();

    /// <summary>
    /// Adds a new node to the hash ring at a position determined by hashing the node key.
    /// </summary>
    /// <param name="key">
    /// The unique identifier for the node to add. Must be a non-null, non-empty string 
    /// that doesn't consist only of whitespace characters.
    /// </param>
    /// <returns>
    /// <c>true</c> if the node was successfully added to the ring; 
    /// <c>false</c> if a node with the same hash already exists (hash collision).
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

        var hash = this.Hash(key);
        _lock.EnterWriteLock();
        try
        {
            var index = _hashList.BinarySearch(hash);
            if (index >= 0) return false; // node position collision

            /* From the BinarySearch docs...
             * [if the index is negative it] contains the bitwise complement
             * of the index of the next element that is larger than item or,
             * if there is no larger element, the bitwise complement of Count.
             */
            index = ~index;
            _hashList.Insert(index, hash);
            _nodeList.Insert(index, key);
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
    /// <c>true</c> if the node was found and successfully removed from the ring; 
    /// <c>false</c> if no node with the specified key exists in the ring.
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
        var hash = this.Hash(key);
        _lock.EnterWriteLock();
        try
        {
            var index = _hashList.BinarySearch(hash);
            if (index < 0) return false; // there is no node with the given key
            _hashList.RemoveAt(index);
            _nodeList.RemoveAt(index);
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
    public string FindNodeFor(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        if (_nodeList.Count == 0)
            throw new InvalidOperationException("The list of nodes is empty. Add one or more nodes");

        var hash = this.Hash(key);
        _lock.EnterReadLock();
        try
        {
            var index = _hashList.BinarySearch(hash);
            if (index >= 0) return _nodeList[index];
            index = ~index;
            if (index == _hashList.Count) return _nodeList[0];
            return _nodeList[index];
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private int Hash(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, nameof(key));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return BitConverter.ToInt32(bytes, 0);
    }
}