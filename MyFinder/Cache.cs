using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyFinder
{
    public class Cache<T> where T : IComparable
    {
        StreamManager _streamManager;
        List<BTreeNode<T>> _nodes;
        bool _enable;
        Action<BinaryWriter, T> _writeKeyAction;
        Func<BinaryReader, T> _readKeyFunc;
        int _keySize;

        public Cache(int pMaxCountNodes, StreamManager pStreamManager, bool pEnable, Action<BinaryWriter, T> pWriteKeyAction, Func<BinaryReader, T> pReadKeyFunc, int pKeySize)
        {
            _nodes = new List<BTreeNode<T>>();
            MaxCountNodes = pMaxCountNodes;
            _streamManager = pStreamManager;
            _enable = pEnable;
            _writeKeyAction = pWriteKeyAction;
            _readKeyFunc = pReadKeyFunc;
            _keySize = pKeySize;
        }

        public bool IsFull { get { return _nodes.Count == MaxCountNodes; } }

        public int MaxCountNodes { get; private set; }

        private BTreeNode<T> RemoveLessUsed(out int pPosRemove)
        {
            BTreeNode<T> lessUsed = _nodes[0];
            pPosRemove = 0;
            for (int i = 0; i < _nodes.Count; i++)
            {
                BTreeNode<T> node = _nodes[i];
                if (node.CountUses < lessUsed.CountUses)
                {
                    lessUsed = node;
                    pPosRemove = i;
                }
            }

            _nodes.RemoveAt(pPosRemove);
            if (lessUsed.Modified)
            {
                lessUsed.Modified = false;
                _streamManager.WriteNode<T>(lessUsed, _writeKeyAction, _keySize);
            }
            return lessUsed;
        }

        private int BinarySearch(long pPosition, int pInf, int pSup, ref bool pFounded)
        {
            if (pInf > pSup)
            {
                pFounded = false;
                return pInf;
            }

            int middle = pInf + (pSup - pInf) / 2;

            if (pPosition > _nodes[middle].BlockPosition)
                return BinarySearch(pPosition, middle + 1, pSup, ref pFounded);

            else if (pPosition < _nodes[middle].BlockPosition)
                return BinarySearch(pPosition, pInf, middle - 1, ref pFounded);

            else
            {
                pFounded = true;
                return middle;
            }
        }

        public void Close()
        {
            if (_enable)
                foreach (var node in _nodes)
                {
                    if (node.Modified)
                    {
                        node.Modified = false;
                        _streamManager.WriteNode<T>(node, _writeKeyAction, _keySize);
                    }
                }
        }

        public BTreeNode<T> GetNode(long pPosition, int pMaxKeysCount)
        {
            if (!_enable)
                return _streamManager.ReadNode<T>(pPosition, pMaxKeysCount, _readKeyFunc);

            bool founded = false;
            int index = BinarySearch(pPosition, 0, _nodes.Count - 1, ref founded);
            BTreeNode<T> node = null;
            if (!founded)
            {
                node = _streamManager.ReadNode<T>(pPosition, pMaxKeysCount, _readKeyFunc);

                int posRemove = 0;
                BTreeNode<T> lessUsed = IsFull ? RemoveLessUsed(out posRemove) : null;

                if (lessUsed != null && posRemove < index)
                    index--;

                _nodes.Insert(index, node);

                return node;
            }

            node = _nodes[index];
            node.CountUses++;
            return node;
        }

        public void SaveNode(BTreeNode<T> pNode)
        {
            if (!_enable)
            {
                pNode.BlockPosition = _streamManager.WriteNode<T>(pNode, _writeKeyAction, _keySize);
                return;
            }

            if (pNode.BlockPosition == 0)
            {
                pNode.Modified = false;
                pNode.BlockPosition = _streamManager.WriteNode<T>(pNode, _writeKeyAction, _keySize);
            }
            bool founded = false;
            int index = BinarySearch(pNode.BlockPosition, 0, _nodes.Count - 1, ref founded);

            if (founded)
            {
                pNode.Modified = true;
                return;// Nodes[index] = pNode;//No necesario modificarlo si lo tengo porq son por ref
            }

            int posRemove = 0;

            BTreeNode<T> lessUsed = IsFull ? RemoveLessUsed(out posRemove) : null;

            if (lessUsed != null && posRemove < index)
                index--;

            pNode.Modified = true;
            _nodes.Insert(index, pNode);
        }

        public BTreeNode<T> ReadRoot(int pBTreeIndex, int pMaxKeysCount)
        {
            return _streamManager.ReadRoot<T>(pBTreeIndex, pMaxKeysCount, _readKeyFunc);
        }

        public void WriteRootBlockPosition(int pBTreeIndex, long pRootBlockPosition)
        {
            _streamManager.WriteRootBlockPosition(pBTreeIndex, pRootBlockPosition);
        }
    }
}
