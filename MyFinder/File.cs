using EDA_PROJECT_1415;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyFinder
{
    public class File:IFile
    {
        public string Address { get; set; }
        public long Size { get; set; }
        public DateTime CreationDate { get; set; }

        public override string ToString()
        {
            return "-----------\nAddress:" + Address + "\nSize:" + Size + "\nCreation Date:" + CreationDate;
        }

        public override bool Equals(object pObj)
        {
            File nFile= (File)pObj;
            if (this.Address != nFile.Address)
                return false;
            if (this.Size != nFile.Size)
                return false;
            if (this.CreationDate != nFile.CreationDate)
                return false;

            return true;
        }
    }
}
