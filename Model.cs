using System.Collections.Generic;
using System.Linq;

namespace mysqldump2mssql
{
    class Column
    {
        public string Name;
        public string MySqlDatatype;
        public string TSqlDatatype;
        public bool Nullable;
        public string Default;
        public string Comment;
        public bool Identity;
    }

    class Key
    {
        public string Name;
        public List<string> Columns;
    }

    class ForeignKey
    {
        public string Name;
        public string RefTable;

        public class Column
        {
            public string Name;
            public string RefName;
        }

        public List<Column> Columns;
        public string OnDelete;
        public string OnUpdate;
    }

    class Table
    {
        public string Schema;
        public string Name;
        public bool Utf8;
        public string Comment;

        public readonly List<Column> Columns = new List<Column>();

        public readonly List<string> PrimaryKeyColumns = new List<string>();
        public readonly List<Key> UniqueKeys = new List<Key>();
        public readonly List<Key> Keys = new List<Key>();
        public readonly List<ForeignKey> ForeignKeys = new List<ForeignKey>();
        public readonly List<Key> FulltextKeys = new List<Key>();

        public bool IdentityInsert { get { return Columns.Any(c => c.Identity); } }

        public Column ColumnByName(string name) => Columns.FirstOrDefault(c => string.Compare(c.Name, name, true) == 0);
    }
}
