namespace mysqldump2mssql
{
    public enum SqlVersion { Sql2005, Sql2008, Sql2016 };

    public static class ConfigInfo
    {
        public static SqlVersion SqlVersion = SqlVersion.Sql2008;
        public static bool DoubleDecodeStrings = false;

        public static string SchemaName = "dbo";
        public static string DateZeroDefault = "1800-01-01 00:00:00";
        public static string SeqFmt = "D3";
    }
}
