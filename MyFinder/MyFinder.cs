using System;
using System.IO;
using System.Collections.Generic;
using EDA_PROJECT_1415;

namespace MyFinder
{
    public class MyFinder : IFinder
    {

        BTreeStringBased _bTreeFileAddress;
        BTreeLongBased _bTreeFileSize;
        BTreeDateTimeBased _bTreeFileCreationDate;

        StreamManager _streamManager;

        public void Open(Stream db)
        {
            _streamManager = new StreamManager(db);

            int maxKeyCount = 3001;
            _bTreeFileAddress = new BTreeStringBased(0, maxKeyCount, _streamManager);
            _bTreeFileSize = new BTreeLongBased(1, maxKeyCount, _streamManager);
            _bTreeFileCreationDate = new BTreeDateTimeBased(2, maxKeyCount, _streamManager);

        }

        public void Close()
        {
            _bTreeFileAddress.Close();
            _bTreeFileSize.Close();
            _bTreeFileCreationDate.Close();
            _streamManager.Close();
        }

        public void AddFile(IFile file)
        {
            long filePosition = _streamManager.WriteFile(file);

            _bTreeFileAddress.Insert(file.Address, filePosition);
            _bTreeFileSize.Insert(file.Size, filePosition);
            _bTreeFileCreationDate.Insert(file.CreationDate, filePosition);

        }

        public bool FindByAddress(string address)
        {
           return _bTreeFileAddress.Search(address)!=-1;
        }
        public IEnumerable<IFile> FindFilesIn(string directoryAddress)
        {
            var backslash=@"\";
            if (directoryAddress.Length>0 && directoryAddress[directoryAddress.Length - 1].ToString() != backslash)
                directoryAddress += backslash;

            foreach (var fileBlockPosition in _bTreeFileAddress.SearchInRange(directoryAddress, directoryAddress, new Func<string, string>(address =>address.Length<directoryAddress.Length?"":address.Substring(0, directoryAddress.Length)))) 
                yield return _streamManager.ReadFile(fileBlockPosition);
        }

        public IEnumerable<IFile> FindByDate(DateTime from, DateTime to)
        {
            foreach (var fileBlockPosition in _bTreeFileCreationDate.SearchInRange(from, to))
                yield return _streamManager.ReadFile(fileBlockPosition);
        }

        public IEnumerable<IFile> FindBySize(long size)
        {
            foreach (var fileBlockPosition in _bTreeFileSize.SearchInRange(size, size))
                yield return _streamManager.ReadFile(fileBlockPosition);
        }

        public IEnumerable<IFile> FindLarger(long size)
        {
            foreach (var fileBlockPosition in _bTreeFileSize.SearchInRange(size + 1, long.MaxValue))
                yield return _streamManager.ReadFile(fileBlockPosition);
        }

        public IEnumerable<IFile> FindSmaller(long size)
        {
            foreach (var fileBlockPosition in _bTreeFileSize.SearchInRange(0, size - 1))
                yield return _streamManager.ReadFile(fileBlockPosition);
        }
    }
}
