using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyFinder
{
    public abstract class BTree<T> where T:IComparable
    {
        BTreeNode<T> _root;
        /// Index of BTree general info on Stream. Used by streamManager to find RootBlokPosition on Stream
        int _bTreeIndex;
        Cache<T> _cache;

        public BTree(int pBTreeIndex, int pMaxKeysCount, StreamManager pStreamManager, Action<BinaryWriter, T> pWriteKeyAction, Func<BinaryReader,T> pReadKeyFunc, int pKeySize)
        {
            if (pMaxKeysCount % 2 == 0)
                throw new Exception("The parameter pMaxKeysCount must be an odd number");

            _bTreeIndex = pBTreeIndex;
            _cache= new Cache<T>(100, pStreamManager,true,pWriteKeyAction,pReadKeyFunc,pKeySize);

            //Is asigned to root what is in the possition of the root of this bTree, if the bTree is empty this value will be null amd it has to be written
            _root = _cache.ReadRoot(pBTreeIndex, pMaxKeysCount);
            if (_root == null)
            {
                _root = new BTreeNode<T>(pMaxKeysCount);
                _cache.SaveNode(_root);
                _cache.WriteRootBlockPosition(_bTreeIndex, _root.BlockPosition);
            }
        }

        #region Properties
        public int MaxKeysCount { get { return _root.MaxKeysCount; } }

        public int MinKeysCount { get { return (MaxKeysCount - 1) / 2; } }

        #endregion

        #region Methods

        /// <summary>
        /// Writing root block position and closing cache
        /// </summary>
        public void Close()
        {
            _cache.WriteRootBlockPosition(_bTreeIndex, _root.BlockPosition);
            _cache.Close();
        }

        /// <summary>
        /// Insert a Key and a Value (long direction of the file) in the B-Tree. If the root is full it is split if not it call with it the methods that insert 
        /// </summary>
        /// <param name="pKey"></param>
        /// <param name="pValue"></param>
        public void Insert(T pKey, long pValue)
        {
            //If he root is full, create a new root and make SplitNode to the old one
            if (_root.IsFull)
            {
                BTreeNode<T> newRoot = new BTreeNode<T>(MaxKeysCount);
                newRoot.AddChild(_root.BlockPosition);
                SplitNode(newRoot, 0, _root);

                _root = newRoot;
                _cache.WriteRootBlockPosition(_bTreeIndex, _root.BlockPosition);
            }
            InsertNonFullNode(pKey, pValue, _root);
        }

        /// <summary>
        /// Split a full node, creating a new one
        /// </summary>
        /// <param name="pParent"></param>
        /// <param name="pParentNewKeyPosition"></param>
        /// <param name="pNode"></param>
        /// <returns></returns>
        private BTreeNode<T> SplitNode(BTreeNode<T> pParent, int pParentNewKeyPosition, BTreeNode<T> pNode)
        {
            BTreeNode<T> newNode = new BTreeNode<T>(MaxKeysCount);

            //Copy the second half of the keys, values and children (if it have any) of the full Node to the newNode
            for (int i = MinKeysCount+1; i < MaxKeysCount; i++)
                newNode.AddKeyValue(pNode.Keys[i], pNode.Values[i]);
            
            if (!pNode.IsLeaf)
                for (int i = MinKeysCount+1; i <= MaxKeysCount; i++)
                    newNode.AddChild(pNode.Children[i]);

            _cache.SaveNode(newNode);

            //Copy the middle key, value and child (if it have any) of the full Node to the newNode
            pParent.InsertKeyValueAt(pParentNewKeyPosition, pNode.Keys[MinKeysCount], pNode.Values[MinKeysCount]);
            pParent.InsertChildAt(pParentNewKeyPosition + 1, newNode.BlockPosition);

            //Delete the keys,  values and children (if it have any) alredy copy to the others Nodes from the full one
            pNode.Keys.RemoveRange(MinKeysCount, MinKeysCount + 1);
            pNode.Values.RemoveRange(MinKeysCount, MinKeysCount + 1);

            if (!pNode.IsLeaf)
                pNode.Children.RemoveRange(MinKeysCount+1, MinKeysCount + 1);

            _cache.SaveNode(pNode);
            _cache.SaveNode(pParent);

            return newNode;
        }

        /// <summary>
        ///Insert a Key and a Value in a non full node
        /// </summary>
        /// <param name="pKey"></param>
        /// <param name="pValue"></param>
        /// <param name="pNode"></param>
        public void InsertNonFullNode(T pKey, long pValue, BTreeNode<T> pNode)
        {
            bool founded = false;
            int index = BinarySearch(pNode.Keys, pKey, 0, pNode.Keys.Count-1,ref founded);

            //If Node is a leaf insert at the corret position
            if (pNode.IsLeaf)
            {
                pNode.InsertKeyValueAt(index , pKey, pValue);
                _cache.SaveNode(pNode);
            }

            //if the node is not a leaf read the corret child and call the method with it
            else
            {
                BTreeNode<T> child = _cache.GetNode(pNode.Children[index], MaxKeysCount);

                if (child.IsFull)
                {
                    BTreeNode<T> newChild = SplitNode(pNode, index, child);

                    if (pKey.CompareTo(pNode.Keys[index]) > 0)
                    {
                        InsertNonFullNode(pKey, pValue, newChild);
                        return;
                    }
                }

                InsertNonFullNode(pKey, pValue, child);
            }
        }

        /// <summary>
        /// Search for an especific Value in the B-Tree
        /// </summary>
        /// <param name="pKey"></param>
        /// <returns></returns>
        public long Search(T pKey)
        {
            return Search(_root, pKey);
        }

        private long Search(BTreeNode<T> pNode, T pKey)
        {
            bool found = false;
            int pIndex = BinarySearch(pNode.Keys, pKey, 0, pNode.Keys.Count - 1, ref found);

            if (!found)
            {
                if (pNode.IsLeaf)
                    return -1;

                else
                {
                    BTreeNode<T> child = _cache.GetNode(pNode.Children[pIndex], MaxKeysCount);
                    return Search(child, pKey);
                }
            }

            else return pNode.Values[pIndex];
        }

        private static int BinarySearch(List<T> pNodesKeys, T pKey, int pInf, int pSup, ref bool pFound)
        {
            if (pInf > pSup)
            {
                pFound = false;
                return pInf;
            }

            int middle = pInf + (pSup - pInf) / 2;

            if (pKey.CompareTo(pNodesKeys[middle]) > 0)
                return BinarySearch(pNodesKeys, pKey, middle + 1, pSup, ref pFound);

            else if (pKey.CompareTo(pNodesKeys[middle]) < 0)
                return BinarySearch(pNodesKeys, pKey, pInf, middle - 1, ref pFound);

            else
            {
                pFound = true;
                return middle;
            }
        }


        /// <summary>
        /// Search for a group of Values in the B-Tree that are in a rage 
        /// </summary>
        /// <param name="pInitialKey"></param> lower bound of the range
        /// <param name="pFinalKey"></param>upper bound of the range
        /// <returns></returns>
        public IEnumerable<long> SearchInRange(T pInitialKey, T pFinalKey)
        {
            return SearchInRange(pInitialKey, pFinalKey, _root, new Func<T, T>(key => key));
        }

        public IEnumerable<long> SearchInRange(T pInitialKey, T pFinalKey, Func<T, T> pKeyPreProcess)
        {
            return SearchInRange(pInitialKey, pFinalKey, _root, pKeyPreProcess);
        }

        public IEnumerable<long> SearchInRange(T pInitialKey, T pFinalKey, BTreeNode<T> pNode, Func<T, T> pKeyPreProcess)
        {
            int i = 0;
            for (i = 0; i < pNode.Keys.Count; i++)
            {
                if (pKeyPreProcess(pNode.Keys[i]).CompareTo(pInitialKey) >= 0 && pKeyPreProcess(pNode.Keys[i]).CompareTo(pFinalKey) <= 0)
                {
                    if (!pNode.IsLeaf)
                    {
                        BTreeNode<T> child = _cache.GetNode(pNode.Children[i], MaxKeysCount);
                        foreach (var value in SearchInRange(pInitialKey, pFinalKey, child, pKeyPreProcess))
                            yield return value;
                    }
                    yield return pNode.Values[i];
                }
                else if (pNode.Keys[i].CompareTo(pFinalKey) > 0) break;

            }
            if (!pNode.IsLeaf)
            {
                BTreeNode<T> child = _cache.GetNode(pNode.Children[i], MaxKeysCount);

                foreach (var value in SearchInRange(pInitialKey, pFinalKey, child, pKeyPreProcess))
                    yield return value;
            }
        }

        #endregion
    }

    public class BTreeStringBased : BTree<string>
    {
        public BTreeStringBased(int pBTreeIndex, int pMaxKeysCount, StreamManager pStreamManager)
            : base(pBTreeIndex, pMaxKeysCount, pStreamManager,
                   new Action<BinaryWriter, string>((bWriter, key) => bWriter.Write(key)),
                   new Func<BinaryReader, string>(bReader=>bReader.ReadString()),258)
        {
        }
    }

    public class BTreeLongBased : BTree<long>
    {
        public BTreeLongBased(int pBTreeIndex, int pMaxKeysCount, StreamManager pStreamManager)
            : base(pBTreeIndex, pMaxKeysCount, pStreamManager,
                   new Action<BinaryWriter, long>((bWriter, key) => bWriter.Write(key)),
                   new Func<BinaryReader, long>(bReader => bReader.ReadInt64()),8)
        {
        }
    }

    public class BTreeDateTimeBased : BTree<DateTime>
    {
        public BTreeDateTimeBased(int pBTreeIndex, int pMaxKeysCount, StreamManager pStreamManager)
            : base(pBTreeIndex, pMaxKeysCount, pStreamManager,
                   new Action<BinaryWriter, DateTime>((bWriter, key) => bWriter.Write((key).Ticks)),
                   new Func<BinaryReader, DateTime>(bReader => new DateTime(bReader.ReadInt64())),8)
        {
        }
    }
}
