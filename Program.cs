using System;
using System.IO;

namespace mysqldump2mssql
{
    static class Program
	{
        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                if (File.Exists(args[0]))
                    MySqlParser.ParseFile(args[0]);
                else
                    Console.WriteLine("File does not exist");
                return;
            }

            Console.WriteLine("mysqldump file required");
        }
    }
}
