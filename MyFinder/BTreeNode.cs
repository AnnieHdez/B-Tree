using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyFinder
{
    public class BTreeNode<T> where T : IComparable
    {
        public BTreeNode(int pMaxKeysCount)
        {
            Keys = new List<T>(pMaxKeysCount);
            Values = new List<long>(pMaxKeysCount);
            Children = new List<long>(pMaxKeysCount + 1);
            MaxKeysCount = pMaxKeysCount;
        }

        #region Properties

        public List<T> Keys { get; private set; }

        /// <summary>
        /// File positions at Stream
        /// </summary>
        public List<long> Values { get; private set; }

        /// <summary>
        /// Children positions at Stream
        /// </summary>
        public List<long> Children { get; private set; }

        public int MaxKeysCount { get; set; }

        /// <summary>
        /// Node position at Stream
        /// </summary>
        public long BlockPosition { get; set; }

        public bool IsLeaf { get { return Children.Count == 0; } }

        public bool IsFull { get { return Keys.Count == MaxKeysCount; } }

        /// <summary>
        /// For cache propouses
        /// </summary>
        public bool Modified { get; set; }

        /// <summary>
        /// For cache propouses
        /// </summary>
        public int CountUses { get; set; }

        #endregion

        #region Methods
        public void AddKeyValue(T pKey, long pValue)
        {
            if (Keys.Count == MaxKeysCount)
                throw new Exception("Trying insert a key/value at Full Node");
            Keys.Add(pKey);
            Values.Add(pValue);
        }

        public void InsertKeyValueAt(int pIndex, T pKey, long pValue)
        {
            if (pIndex != 0 && (pIndex < 0 || pIndex > Keys.Count))
                throw new Exception("Index out of range");
            if (Keys.Count == MaxKeysCount)
                throw new Exception("Trying insert a child at Full Node");
            Keys.Insert(pIndex, pKey);
            Values.Insert(pIndex, pValue);
        }

        public void AddChild(long pChild)
        {
            if (Children.Count == MaxKeysCount + 1)
                throw new Exception("Trying insert a child at Full Node");
            Children.Add(pChild);
        }

        public void InsertChildAt(int pIndex, long pChild)
        {
            if (pIndex != 0 && (pIndex < 0 || pIndex > Children.Count))
                throw new Exception("Index out of range");
            if (Children.Count == MaxKeysCount + 1)
                throw new Exception("Trying insert a child at Full Node");
            Children.Insert(pIndex, pChild);
        }

        
        public override string ToString()
        {
            string nodeInfo = "Position:" + BlockPosition+"\n";

            nodeInfo += "Keys: ";

            for (int i = 0; i < Keys.Count - 1; i++)
            {
                nodeInfo += Keys[i];
                nodeInfo += ", ";
            }

            if (Keys.Count != 0)
                nodeInfo += Keys[Keys.Count - 1];
            nodeInfo += "\n";

             nodeInfo += "Values:  ";

            for (int i = 0; i < Keys.Count - 1; i++)
             {
                 nodeInfo += Values[i];
                 nodeInfo += ", ";
             }
             if (Keys.Count != 0)
                 nodeInfo += Values[Keys.Count - 1];
             nodeInfo += "\n";

             nodeInfo += "Children:  ";

             for (int i = 0; i < Children.Count - 1; i++)
             {
                 nodeInfo += Children[i];
                 nodeInfo += ", ";
             }
             if (Children.Count != 0)
                 nodeInfo += Children[Children.Count - 1];
             nodeInfo += "\n";
  
            return nodeInfo;
        }
        #endregion
    }

}
