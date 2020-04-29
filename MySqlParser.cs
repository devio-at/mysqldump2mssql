using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace mysqldump2mssql
{
    public static class MySqlParser
    {
        public static bool ParseFile(string filename)
        {
            var directoryName = Path.ChangeExtension(filename, ".mssql");
            Directory.CreateDirectory(directoryName);

            var rexDropTable = new Regex(@"^DROP\s+TABLE\s+IF\s+EXISTS\s+`(?<name>[\w_]+)`;$", RegexOptions.IgnoreCase);
            var rexCreateTable = new Regex(@"^CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?`(?<name>[\w_]+)`\s+\($", RegexOptions.IgnoreCase);
            var rexTableColumn = new Regex(@"^\s*`(?<name>[\w_]+)`\s+
    (?<datatype>
        (\w+(\(\d+(,\d+)?\))?)
        |  (ENUM\s*\('[^']*'(,'[^']*')*\))
        |  (SET\s*\('[^']*'(,'[^']*')*\))
    )
    (\s+CHARACTER\s+SET\s+(?<charset>[\w_]+))?
    (\s+COLLATE\s+(?<collate>[\w_]+))?
    (\s+UNSIGNED)?
    (\s+(?<null>((NOT\s+NULL)|NULL)))?
    (\s+(?<autoincrement>AUTO_INCREMENT))?
    (\s+DEFAULT\s+(?<default>(NULL|\'[^']*\'|   (CURRENT_TIMESTAMP(\(\d+\))?) )))?
    (\s+ON\s+UPDATE\s+CURRENT_TIMESTAMP(\(\d+\))?)?
    (\s+COMMENT\s+(?<comment>\'[^']*\'))?(,)?$",
                RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);
            var rexPrimaryKey = new Regex(@"^\s*PRIMARY\s+KEY\s+\(`(?<name>[\w_]+)`(\(\d+\))?(,`(?<name>[\w_]+)`(\(\d+\))?)*\)\s*(,)?$", RegexOptions.IgnoreCase);
            var rexTableColumnEnd = new Regex(@"^\)
    (\s+(
        (ENGINE=(?<engine>[\w_]+))
      | (AUTO_INCREMENT=\d+)
      | (DEFAULT\s+CHARSET=(?<charset>[\w_]+))
      | (COLLATE=(?<collate>[\w_]+))
      | (MAX_ROWS=\d+)
      | (AVG_ROW_LENGTH=\d+)
      | (COMMENT=(?<comment>\'[^']*\'))
      | (STATS_PERSISTENT=\d+)
      | (ROW_FORMAT=\w+)
    ))*;$",
                RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);
            var srexValue = @"(?<value>((_binary\s+)?\'((\\')|[^'])*\'|\-?\d+(\.\d+)?|NULL))";
            var srexValueSet = @"(?<values>\(" + srexValue + @"(," + srexValue + @")*\))";
            var rexInsert = new Regex(@"^INSERT\s+INTO\s+`(?<name>[\w_]+)`\s+VALUES\s*" + srexValueSet + @"(\s*,\s*" + srexValueSet + ")*;$", RegexOptions.IgnoreCase);
            var rexValues = new Regex(srexValueSet);
            var rexUniqueKey = new Regex(@"^\s*UNIQUE\s+KEY\s+`(?<name>[\w_]+)`\s+
    \(`(?<column>[\w_]+)`(\(\d+\))?
    (,`(?<column>[\w_]+)`(\(\d+\))?)*\)\s*(,)?$",
                RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);
            var rexKey = new Regex(@"^\s*KEY\s\`(?<name>[\w_]+)`\s\(`(?<column>[\w_]+)`(\(\d+\))?(,`((?<column>[\w_]+)`)(\(\d+\))?)*\)\s*(,)?$", RegexOptions.IgnoreCase);
            var rexForeignKey = new Regex(@"^\s*CONSTRAINT\s+`(?<name>[\w_]+)`\s+
    FOREIGN\s+KEY\s*\(`(?<column>[\w_]+)`(,`(?<column>[\w_]+)`)*\)\s*
    REFERENCES\s+`(?<refname>[\w_]+)`\s+\(`(?<refcolumn>[\w_]+)`(,`(?<refcolumn>[\w_]+)`)*\)
    (\s+ON\s+DELETE\s+(?<delete>CASCADE|SET\s+NULL))?
    (\s+ON\s+UPDATE\s+(?<update>CASCADE|SET\s+NULL))?\s*(,)?$",
                RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace);
            var rexFulltextKey = new Regex(@"^\s*FULLTEXT\s+KEY\s+`(?<name>[\w_]+)`\s+\(`(?<column>[\w_]+)`(,`(?<column>[\w_]+)`)*\)\s*(,)?$", RegexOptions.IgnoreCase);

            var skipStartsWith = new[] { "--", "/*!40", "SET @saved_cs_client ", "SET character_set_client " };
            var skipRegex = new[] { new Regex(@"^CREATE\s+DATABASE\s+", RegexOptions.IgnoreCase), new Regex(@"^USE\s+", RegexOptions.IgnoreCase) };

            var dTables = new Dictionary<string, Table>(StringComparer.CurrentCultureIgnoreCase);
            var dTableCreateFilename = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            var dTableDataFilename = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            Table t = null;
            StreamWriter wr = null;
            var seq = 0;

            using (var f = new StreamReader(filename, Encoding.UTF8))
            {
                string line;

                Match m;

                while ((line = f.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    else if (skipStartsWith.Any(l => line.StartsWith(l))) continue;
                    else if (skipRegex.Any(r => r.Match(line).Success)) continue;

                    else if ((m = rexDropTable.Match(line)).Success)
                    {
                        if (wr != null)
                        {
                            wr.Flush();
                            wr.Close();
                        }

                        var tablename = m.Groups["name"].Value;
                        if (!dTableCreateFilename.ContainsKey(tablename))
                        {
                            var sqlfilename = Path.Combine(directoryName, seq.ToString(ConfigInfo.SeqFmt) + " " + tablename + " create.sql");
                            seq++;
                            dTableCreateFilename.Add(tablename, sqlfilename);
                            wr = new StreamWriter(sqlfilename, false, Encoding.UTF8);
                        }
                        else
                            wr = new StreamWriter(dTableCreateFilename[tablename], true, Encoding.UTF8);

                        if (ConfigInfo.SqlVersion >= SqlVersion.Sql2016)
                            wr.WriteLine(string.Format(@"DROP TABLE IF EXISTS {0};", ObjectName(tablename)));
                        else
                            wr.WriteLine(string.Format(@"IF OBJECT_ID('{0}', 'U') IS NOT NULL 
	DROP TABLE {0};", ObjectName(tablename)));
                    }
                    else if ((m = rexCreateTable.Match(line)).Success)
                    {
                        t = new Table { Name = m.Groups["name"].Value, Schema = ConfigInfo.SchemaName };
                        dTables.Add(t.Name, t);
                    }
                    else if ((m = rexTableColumn.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("table column without table");
                            goto End;
                        }
                        var c = new Column
                        {
                            Name = m.Groups["name"].Value,
                            MySqlDatatype = m.Groups["datatype"].Value,
                            Nullable = string.Compare(m.Groups["null"].Value, "NOT NULL", true) != 0,
                            Default = m.Groups["default"].Value,
                            Comment = m.Groups["comment"].Value,
                            Identity = !string.IsNullOrEmpty(m.Groups["autoincrement"].Value)
                        };
                        var dt = TranslateDatatype(c.MySqlDatatype);

                        if (dt == null)
                        {
                            Console.WriteLine("unknown data type " + c.MySqlDatatype);
                            goto End;
                        }

                        c.TSqlDatatype = dt;
                        t.Columns.Add(c);
                    }
                    else if ((m = rexPrimaryKey.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("primary key without table");
                            goto End;
                        }

                        t.PrimaryKeyColumns.AddRange(m.Groups["name"].Captures.Cast<Capture>().Select(c => c.Value));
                    }
                    else if ((m = rexUniqueKey.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("unique key without table");
                            goto End;
                        }

                        t.UniqueKeys.Add(new Key
                        {
                            Name = m.Groups["name"].Value,
                            Columns = m.Groups["column"].Captures.Cast<Capture>().Select(c => c.Value).ToList()
                        });
                    }
                    else if ((m = rexFulltextKey.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("fulltext key without table");
                            goto End;
                        }

                        t.FulltextKeys.Add(new Key
                        {
                            Name = m.Groups["name"].Value,
                            Columns = m.Groups["column"].Captures.Cast<Capture>().Select(c => c.Value).ToList()
                        });
                    }
                    else if ((m = rexKey.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("key without table");
                            goto End;
                        }

                        t.Keys.Add(new Key
                        {
                            Name = m.Groups["name"].Value,
                            Columns = m.Groups["column"].Captures.Cast<Capture>().Select(c => c.Value).ToList()
                        });
                    }

                    else if ((m = rexForeignKey.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("foreign key without table");
                            goto End;
                        }

                        var fk = new ForeignKey
                        {
                            Name = m.Groups["name"].Value,
                            RefTable = m.Groups["refname"].Value,
                            OnDelete = m.Groups["delete"].Value,
                            OnUpdate = m.Groups["update"].Value,
                            Columns = new List<ForeignKey.Column>()
                        };
                        var col = m.Groups["column"].Captures.Cast<Capture>().Select(c => c.Value).ToList();
                        var refcol = m.Groups["refcolumn"].Captures.Cast<Capture>().Select(c => c.Value).ToList();

                        for (var ic = 0; ic < col.Count(); ic++)
                            fk.Columns.Add(new ForeignKey.Column { Name = col[ic], RefName = refcol[ic] });

                        t.ForeignKeys.Add(fk);
                    }

                    else if ((m = rexTableColumnEnd.Match(line)).Success)
                    {
                        if (t == null)
                        {
                            Console.WriteLine("table end without table");
                            goto End;
                        }

                        if (wr != null)
                        {
                            wr.Flush();
                            wr.Close();
                        }
                        var tablename = t.Name;
                        if (!dTableCreateFilename.ContainsKey(tablename))
                        {
                            var sqlfilename = Path.Combine(directoryName, seq.ToString(ConfigInfo.SeqFmt) + " " + t.Name + " create.sql");
                            seq++;
                            dTableCreateFilename.Add(tablename, sqlfilename);
                            wr = new StreamWriter(sqlfilename, false, Encoding.UTF8);
                        }
                        else
                            wr = new StreamWriter(dTableCreateFilename[tablename], true, Encoding.UTF8);

                        Console.WriteLine("create table " + t.Name);

                        t.Utf8 = m.Groups["charset"].Value == "utf8";
                        t.Comment = m.Groups["comment"].Value;

                        wr.WriteLine("CREATE TABLE " + ObjectName(t.Name) + " (");
                        foreach (var c in t.Columns)
                        {
                            wr.WriteLine(string.Format("  {0} {1} {2}{3},",
                                QuoteID(c.Name), c.TSqlDatatype, c.Nullable ? "NULL" : "NOT NULL",
                                (c.Identity ? " IDENTITY(1,1)" : !string.IsNullOrEmpty(c.Default) ? " DEFAULT " + ConvertValue(c.TSqlDatatype, c.Default) : "")));
                        }

                        if (t.PrimaryKeyColumns.Count > 0)
                            wr.WriteLine("  CONSTRAINT " + QuoteID("PK_" + t.Name) + " PRIMARY KEY (" + string.Join(", ", t.PrimaryKeyColumns.Select(QuoteID)) + "),");

                        foreach (var uk in t.Keys)
                            wr.WriteLine("  " +
                                (uk.Columns.All(kc => DatatypeAllowedInIndex(t.ColumnByName(kc).TSqlDatatype)) ? "" : "-- ") +
                                "INDEX " + QuoteID(uk.Name) + " (" + string.Join(", ", uk.Columns.Select(QuoteID)) + "),");

                        wr.WriteLine(")");
                        wr.WriteLine("GO");

                        foreach (var uk in t.UniqueKeys)
                        {
                            var valid = uk.Columns.All(kc => {
                                var tc = t.ColumnByName(kc);
                                return !tc.Nullable && DatatypeAllowedInIndex(tc.TSqlDatatype);
                            });
                            wr.WriteLine();
                            if (!valid) wr.WriteLine("/*");
                            wr.WriteLine("CREATE UNIQUE INDEX " + QuoteID(uk.Name) + " ON " + ObjectName(t.Name) + " (" + string.Join(", ", uk.Columns.Select(QuoteID)) + ")");
                            wr.WriteLine("GO");
                            if (!valid) wr.WriteLine("*/");
                        }
                    }
                    else if (line.StartsWith("LOCK TABLES ")) continue; //`actions` WRITE;
                    else if (line == "set autocommit=0;") continue;
                    else if (line.StartsWith("UNLOCK TABLES;")) continue;
                    else if (line.StartsWith("commit;")) continue;
                    else if ((m = rexInsert.Match(line)).Success)
                    {
                        if (wr != null)
                        {
                            wr.Flush();
                            wr.Close();
                        }

                        var tablename = m.Groups["name"].Value;
                        if (!dTableDataFilename.ContainsKey(tablename))
                        {
                            var sqlfilename = Path.Combine(directoryName, seq.ToString(ConfigInfo.SeqFmt) + " " + t.Name + " data.sql");
                            seq++;
                            dTableDataFilename.Add(tablename, sqlfilename);
                            wr = new StreamWriter(sqlfilename, false, Encoding.UTF8);
                        }
                        else
                            wr = new StreamWriter(dTableDataFilename[tablename], true, Encoding.UTF8);

                        Console.WriteLine("insert into " + tablename);

                        var insertTable = dTables[tablename];
                        if (insertTable.IdentityInsert)
                            wr.WriteLine("SET IDENTITY_INSERT " + QuoteID(insertTable.Name) + " ON;");

                        var iRow = 0;
                        var insertInto = "INSERT INTO " + ObjectName(tablename) + " (" + string.Join(", ", insertTable.Columns.Select(c => QuoteID(c.Name))) + ")";

                        wr.WriteLine(insertInto);
                        var first = true;
                        foreach (Capture values in m.Groups["values"].Captures)
                        {
                            var mm = rexValues.Match(values.Value);

                            var columnValues = mm.Groups["value"].Captures.Cast<Capture>().Select(c => c.Value).ToArray();

                            wr.WriteLine((first ? "VALUES" : "     ,") + " ("
                                + string.Join(", ",
                                    Enumerable.Range(0, insertTable.Columns.Count).Select(ic => ConvertValue(insertTable.Columns[ic].TSqlDatatype, columnValues[ic])))
                                + ")");
                                
                            first = false;
                            iRow++;

                            if (iRow == 1000)
                            {
                                wr.WriteLine(";");
                                wr.WriteLine(insertInto);
                                iRow = 0;
                                first = true;
                            }
                        }
                        wr.WriteLine(";");
                        wr.WriteLine("GO");

                        if (insertTable.IdentityInsert)
                            wr.WriteLine("SET IDENTITY_INSERT " + QuoteID(insertTable.Name) + " OFF;");
                    }
                    else
                    {
                        Console.WriteLine("unsupported line:");
                        goto End;
                    }

                    continue;

                    End:
                    Console.WriteLine(line);
                    return false;
                }
                f.Close();

                if (wr != null)
                {
                    wr.Flush();
                    wr.Close();
                }
            }

            foreach (var tt in dTables.Values.Where(ttt => ttt.ForeignKeys.Count > 0).OrderBy(ttt => ttt.Name))
            {
                t = tt;
                var sqlfilename = Path.Combine(directoryName, seq.ToString(ConfigInfo.SeqFmt) + " " + t.Name + " fk.sql");
                seq++;

                Console.WriteLine("create foreign keys " + t.Name);

                wr = new StreamWriter(sqlfilename, false, Encoding.UTF8);

                foreach (var fk in t.ForeignKeys)
                {
                    wr.WriteLine("ALTER TABLE " + (!string.IsNullOrEmpty(t.Schema) ? QuoteID(t.Schema) + "." : "") + QuoteID(t.Name) + @"
ADD CONSTRAINT " + QuoteID(fk.Name) + " FOREIGN KEY (" + string.Join(", ", fk.Columns.Select(c => QuoteID(c.Name)))
+ ") REFERENCES " + QuoteID(fk.RefTable) + " (" + string.Join(", ", fk.Columns.Select(c => QuoteID(c.RefName))) + ")"
+ (!string.IsNullOrEmpty(fk.OnDelete) ? @"
ON DELETE " + fk.OnDelete : "")
+ (!string.IsNullOrEmpty(fk.OnUpdate) ? @"
ON UPDATE " + fk.OnUpdate : ""));
                    wr.WriteLine("GO");
                }

                wr.Flush();
                wr.Close();
            }

            if (dTables.Values.Any(ttt => ttt.FulltextKeys.Count > 0))
            {
                var sqlfilename = Path.Combine(directoryName, seq.ToString(ConfigInfo.SeqFmt) + " fulltext catalog.sql");
                seq++;

                wr = new StreamWriter(sqlfilename, false, Encoding.UTF8);

                wr.WriteLine(@"
CREATE FULLTEXT CATALOG[DefaultCatalog] AS DEFAULT
GO
");
                wr.Flush();
                wr.Close();
            }

            foreach (var tt in dTables.Values.Where(ttt => ttt.FulltextKeys.Count > 0).OrderBy(ttt => ttt.Name))
            {
                t = tt;
                var sqlfilename = Path.Combine(directoryName, seq.ToString(ConfigInfo.SeqFmt) + " " + t.Name + " fulltext.sql");
                seq++;

                Console.WriteLine("create fulltext keys " + t.Name);

                wr = new StreamWriter(sqlfilename, false, Encoding.UTF8);

                if (t.PrimaryKeyColumns.Count == 0)
                {
                    Console.WriteLine("WARNING: no pk to create fulltext keys " + t.Name);
                    wr.WriteLine("-- WARNING: No PK");
                }
                else if (t.PrimaryKeyColumns.Count > 1)
                {
                    Console.WriteLine("WARNING: too many PK columns to create fulltext keys " + t.Name);
                    wr.WriteLine("-- WARNING: Too many PK columns");
                }
                else
                {
                    wr.WriteLine("CREATE FULLTEXT INDEX ON " + (!string.IsNullOrEmpty(t.Schema) ? QuoteID(t.Schema) + "." : "") + QuoteID(t.Name) + @"
(" + string.Join(", ", t.FulltextKeys.SelectMany(ftk => ftk.Columns).Select(c => QuoteID(c))) + @")
KEY INDEX " + QuoteID("PK_" + t.Name));
                }

                wr.Flush();
                wr.Close();
            }

            return true;
        }

        static string QuoteID(string name)
        {
            return "[" + name + "]";
        }

        static string ObjectName(string name)
        {
            return (!string.IsNullOrEmpty(ConfigInfo.SchemaName) ? QuoteID(ConfigInfo.SchemaName) + "." : "") + QuoteID(name);
        }

        static readonly Dictionary<string, string> dDatatypes = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
        {
            { "longblob", "VARBINARY(MAX)" },
            { "text", "NVARCHAR(MAX)" },
            { "longtext", "NVARCHAR(MAX)" },
            { "mediumtext", "NVARCHAR(MAX)" },
            { "blob", "NVARCHAR(MAX)" },
            { "float", "FLOAT" },
            { "datetime", "DATETIME" },
            { "double", "FLOAT" },
            { "int", "INT" },
            { "tinyint", "SMALLINT" },   // signed => unsigned!
            { "smallint", "SMALLINT" },
            { "tinytext", "NVARCHAR(256)" },
            { "mediumblob", "VARBINARY(MAX)" },
            { "tinyblob", "VARBINARY(256)" },
            { "timestamp", "DATETIME" },
            { "time", ConfigInfo.SqlVersion >= SqlVersion.Sql2008 ? "TIME" : "DATETIME" },
            { "date", ConfigInfo.SqlVersion >= SqlVersion.Sql2008 ? "DATE" : "DATETIME" },
        };

        static readonly Dictionary<string, string> dDatatypesLen = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
        {
            { "mediumint(", "INT" },
            { "tinyint(", "SMALLINT" },
            { "int(", "INT" },
            { "smallint(", "SMALLINT" },
            { "bigint(", "BIGINT" },
            { "timestamp(", "DATETIME" },
            { "time(", ConfigInfo.SqlVersion >= SqlVersion.Sql2008 ? "TIME" : "DATETIME" }
        };

        static string TranslateDatatype(string datatype)
        {
            if (dDatatypes.ContainsKey(datatype))
                return dDatatypes[datatype];

            if (datatype.StartsWith("varchar(", StringComparison.InvariantCultureIgnoreCase))
            {
                return "NVARCHAR" + datatype.Substring(7);
            }
            if (datatype.StartsWith("decimal(", StringComparison.InvariantCultureIgnoreCase))
            {
                return "DECIMAL" + datatype.Substring(7);
            }
            if (datatype.StartsWith("char(", StringComparison.InvariantCultureIgnoreCase))
            {
                return "NCHAR" + datatype.Substring(4);
            }
            if (datatype.StartsWith("varbinary(", StringComparison.InvariantCultureIgnoreCase))
            {
                return "VARBINARY" + datatype.Substring(9);
            }
            if (datatype.StartsWith("binary(", StringComparison.InvariantCultureIgnoreCase))
            {
                return "BINARY" + datatype.Substring(6);
            }
            if (datatype.StartsWith("enum(", StringComparison.InvariantCultureIgnoreCase))
            {
                var rgs = datatype.Substring(5).Split(",".ToCharArray());
                var l = rgs.Select(v => v.Length).Max();
                return "VARCHAR(" + (l - 2) + ")";
            }
            if (datatype.StartsWith("set(", StringComparison.InvariantCultureIgnoreCase))
            {
                return "BIGINT";
            }

            var dtLens = dDatatypesLen.Where(kv => datatype.StartsWith(kv.Key, StringComparison.InvariantCultureIgnoreCase)).ToArray();
            if (dtLens.Length > 0)
                return dtLens[0].Value;

            return null;
        }

        static bool DatatypeAllowedInIndex(string datatype)
        {
            return datatype != "NVARCHAR(MAX)";
        }

        static Regex UnescapeRex = new Regex(@"\\([0-9a-fA-F]{1,2})");

        static string UnescapeBinary(string value)
        {
            // SO 11584148
            var result = new StringBuilder();

            var pos = 0;
            foreach (Match m in UnescapeRex.Matches(value))
            {
                result.Append(value.Substring(pos, m.Index - pos));
                pos = m.Index + m.Length;
                result.Append((char)Convert.ToInt32(m.Groups[1].ToString(), 16));
            }
            result.Append(value.Substring(pos));
            return "0x" + string.Join("", result.ToString().Select(c => ((int)c).ToString("X2")));
        }

        static string ConvertValue(string tsqlDatatype, string value)
        {
            if (value == "NULL") return value;

            if (tsqlDatatype.Contains("BINARY"))
            {
                //   [bp_token] BINARY(32) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
                if (value == "''") return "0x";

                if (value.StartsWith("_binary"))
                    value = value.Substring(7).Trim();

                return UnescapeBinary(value.Substring(1, value.Length - 2));      // trim quotes
            }
            if (tsqlDatatype.Contains("INT"))
            {
                if (value[0] == '\'')
                    return value.Substring(1, value.Length - 2);
                return value;
            }
            if (tsqlDatatype.StartsWith("NVARCHAR") || tsqlDatatype.StartsWith("NCHAR"))
            {
                if (value.StartsWith("_binary"))
                    value = value.Substring(7).Trim();

                return "N'" + ConvertQuotedString(value) + "'";
            }
            if (tsqlDatatype == "DATETIME")
            {
                if (value == "'0000-00-00 00:00:00'" || value == "0000-00-00 00:00:00")
                    return "'" + ConfigInfo.DateZeroDefault + "'";

                if (value.StartsWith("'")) return value;
                return "'" + value + "'";
            }
            if (tsqlDatatype == "TIME")
            {
                if (value.StartsWith("'")) return value;
                return "'" + value + "'";
            }
            if (tsqlDatatype == "DATE")
            {
                if (value == "'0000-00-00'" || value == "0000-00-00")
                    return "'" + ConfigInfo.DateZeroDefault.Substring(0, 10) + "'";

                if (value.StartsWith("'")) return value;
                return "'" + value + "'";
            }

            if (value.StartsWith("'"))
                return ConvertQuotedString(value);

            return value;
        }

        static string ConvertQuotedString(string s)
        {
            s = s.Substring(1, s.Length - 2).Replace("\'", "''");

            if (ConfigInfo.DoubleDecodeStrings)
            {
                // handle double UTF8-encoded strings https://www.i18nqa.com/debug/utf8-debug.html
                var bytes = s.Select(c => (byte)c).ToArray();
                var result = Encoding.UTF8.GetString(bytes);
                return result;
            }

            return s;
        }
    }
}
