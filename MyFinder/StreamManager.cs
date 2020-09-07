using EDA_PROJECT_1415;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyFinder
{
    public class StreamManager
    {
        BinaryReader _bReader;
        BinaryWriter _bWriter;
        Stream _stream;
        long _newBlockPosition;

        public StreamManager(Stream pStream)
        {
            _bReader = new BinaryReader(pStream);
            _bWriter = new BinaryWriter(pStream);
            _stream = pStream;
            
            //Read NewBlockPosition from stream
            if (_stream.Length > 1)
            {
                _stream.Seek(0, SeekOrigin.Begin);
                _newBlockPosition = _bReader.ReadInt64();
            }
            else//case Empty Stream => NewBlockPosition = FirstBlockPosition
                _newBlockPosition = 8 + 3 * 8; //NewBlockPosition + Root1Position + Root2Position + Root3Position  (size(long)=8)
        }

        public void Close()
        {
            //Write newBlockPosition to stream
            _stream.Seek(0, SeekOrigin.Begin);
            _bWriter.Write(_newBlockPosition);
            _bWriter.Flush();
        }

        public long WriteFile(IFile pFile)
        {
            long fileBlockPosition = _newBlockPosition;
            _stream.Seek(_newBlockPosition, SeekOrigin.Begin);
            _bWriter.Write(pFile.Address);
            _bWriter.Write(pFile.Size);
            _bWriter.Write(pFile.CreationDate.Ticks);
            _bWriter.Flush();
            _newBlockPosition = _stream.Position;

            return fileBlockPosition;
        }

        public IFile ReadFile(long pFileBlockPosition)
        {
            File file = new File();
            _stream.Seek(pFileBlockPosition, SeekOrigin.Begin);
            file.Address = _bReader.ReadString();
            file.Size = _bReader.ReadInt64();
            file.CreationDate = new DateTime(_bReader.ReadInt64());

            return file;
        }

        private long BlockSize(int pMaxKeysCount, int pKeySize)
        {
            return 4                             //KeysCount (int)
                    + 1                          //IsLeaf    (bool)
                    + pKeySize * pMaxKeysCount   //Keys      (string 256, long, long)
                    + 8 * pMaxKeysCount          //Values    (long)
                    + 8 * (pMaxKeysCount+1);     //Children  (long)
        }

        public long WriteNode<T>(BTreeNode<T> pNode, Action<BinaryWriter,T> pWriteKey, int pKeySize) where T:IComparable
        {
            if (pNode.BlockPosition == 0) //New Block
            {
                pNode.BlockPosition = _newBlockPosition;
                WriteNodeBlock(pNode, pWriteKey);
                _newBlockPosition = pNode.BlockPosition + BlockSize(pNode.MaxKeysCount, pKeySize); //_stream.Position;
            }
            else //Update Block
            {
                WriteNodeBlock(pNode, pWriteKey);
            }

            return pNode.BlockPosition;
        }

        private void WriteNodeBlock<T>(BTreeNode<T> pNode, Action<BinaryWriter,T> pWriteKey) where T:IComparable
        {
            object x = null;
            BTreeNode<T> n = (BTreeNode<T>)x;
            _stream.Seek(pNode.BlockPosition, SeekOrigin.Begin);

            _bWriter.Write(pNode.Keys.Count);
            _bWriter.Write(pNode.IsLeaf);

            foreach (T key in pNode.Keys)
                pWriteKey(_bWriter, key);

            foreach (long value in pNode.Values)
                _bWriter.Write(value);

            foreach (long childBlockPosition in pNode.Children)
                _bWriter.Write(childBlockPosition);

            _bWriter.Flush();
        }

        public BTreeNode<T> ReadNode<T>(long pBlockPosition, int pMaxKeysCount, Func<BinaryReader, T> pReadKeyFunc) where T:IComparable
        {
            BTreeNode<T> node = new BTreeNode<T>(pMaxKeysCount);
            _stream.Seek(pBlockPosition, SeekOrigin.Begin);

            node.BlockPosition = pBlockPosition;

            int keysCount = _bReader.ReadInt32();
            bool isLeaf=_bReader.ReadBoolean();

            for (int i = 0; i < keysCount; i++)
                node.Keys.Add(pReadKeyFunc(_bReader));

            for (int i = 0; i < keysCount; i++)
                node.Values.Add(_bReader.ReadInt64());

            if(!isLeaf)
                for (int i = 0; i < keysCount + 1; i++)
                    node.AddChild(_bReader.ReadInt64());
            return node;
        }

        public BTreeNode<T> ReadRoot<T>(int pBTreeIndex, int pMaxKeysCount, Func<BinaryReader, T> pReadKeyFunc) where T:IComparable
        {
            long position = 8 + pBTreeIndex * 8; //NewBlockPosition + Root1Position+...
            long rootBlockPosition = 0;
            if (_stream.Length >= position + 8)
            {
                _stream.Seek(position, SeekOrigin.Begin);
                rootBlockPosition = _bReader.ReadInt64();
                if (rootBlockPosition == 0)
                    return null;
                return ReadNode(rootBlockPosition, pMaxKeysCount, pReadKeyFunc);
            }
            return null;
        }

        public void WriteRootBlockPosition(int pBTreeIndex, long pRootBlockPosition)
        {
            long position = 8 + pBTreeIndex * 8; //NewBlockPosition + Root1Position+...
            _stream.Seek(position, SeekOrigin.Begin);
            _bWriter.Write(pRootBlockPosition);
            _bWriter.Flush();
        }
    }
}
