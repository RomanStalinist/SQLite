using System.IO;
using System.Data;
using System.Diagnostics;
using System.Collections;
using Microsoft.Data.Sqlite;

#pragma warning disable IDE1006

namespace database.sqlite
{

    public class SQLiteException : SqliteException
    {
        public int errorCode
        {
            get
            {
                return errorCode;
            }
            set
            {
                ArgumentOutOfRangeException.ThrowIfLessThan(value, -1, nameof(value));
                errorCode = value;
            }
        }

        public string message { get; set; } = string.Empty;

        public SQLiteException(): base(string.Empty, -1)
        {
            Debug.WriteLine(StackTrace);
        }
        public SQLiteException(string Message): base(Message, -1)
        {
            message = Message;

            Debug.WriteLine(StackTrace);
        }
        public SQLiteException(string Message, int ErrorCode) : base(Message, ErrorCode)
        {
            message = Message;
            errorCode = ErrorCode;

            Debug.WriteLine($"Error code: {errorCode}");
            Debug.WriteLine(StackTrace);
        }
    }

    public class VersionException: Exception
    {
        public class Versions(int oldversion, int newversion)
        {
            public int oldVersion = oldversion;
            public int newVersion = newversion;
        }

        public VersionException() : base() { }
        public VersionException(string message) : base(message) { }
        public VersionException(int version) : base($"Version is lower than zero: {version}") { }
        public VersionException(int oldVersion, int newVersion) : base($"Old version: {oldVersion}\r\nNew version: {newVersion}") { }
        public VersionException(Versions versions) : base($"Old version: {versions.oldVersion}\r\nNew version: {versions.newVersion}") { }
        public static void ThrowIfEquals(Versions versions)
        {
            if (versions.oldVersion == versions.newVersion)
            {
                throw new VersionException(versions);
            }
        }
        public static void ThrowIfLess(Versions versions)
        {
            if (versions.oldVersion < versions.newVersion)
            {
                throw new VersionException(versions);
            }
        }
        public static void ThrowIfGreater(Versions versions)
        {
            if (versions.oldVersion > versions.newVersion)
            {
                throw new VersionException(versions);
            }
        }
        public static void ThrowIfNegative(int version)
        {
            if (version < 0)
            {
                throw new VersionException(version);
            }
        }
    }

    public class ContentValues
    {
        public Dictionary<string, object?> values { get; set; }
        public ContentValues() => values = [];
        public ContentValues(int size) => values = new(size);
        public ContentValues(ContentValues from) => values = from.values;
        public ContentValues(Dictionary<string, object?> from) => values = from;
        public void clear()
        {
            values.Clear();
        }
        public bool containsKey(string key)
        {
            return values.ContainsKey(key);
        }
        public bool equals(object obj)
        {
            if (obj is ContentValues cv)
            {
                return cv.size() == size() && !cv.values.Except(values).Any();
            }
            return false;
        }
        public object? get(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return values[key];
        }
        public bool getAsBool(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return bool.Parse(values[key].ToString());
        }
        public byte getAsByte(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return Convert.ToByte(values[key]);
        }
        public double getAsDouble(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return Convert.ToDouble(values[key]);
        }
        public float getAsFloat(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return float.Parse(values[key].ToString());
        }
        public int getAsInteger(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return int.Parse(values[key].ToString());
        }
        public long getAsLong(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return long.Parse(values[key].ToString());
        }
        public short getAsShort(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return short.Parse(values[key].ToString());
        }
        public string getAsString(string key)
        {
            if (!containsKey(key))
            {
                throw new SQLiteException($"ContentValues don't have {key} key!");
            }
            return values[key].ToString();
        }
        public bool isEmpty()
        {
            return size() == 0;
        }
        public HashSet<string> keySet()
        {
            return new HashSet<string>(values.Keys);
        }
        public void put(string key, short value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, long value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, double value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, int value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, bool value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, float value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, byte value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, byte[] value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void put(string key, object? value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new SQLiteException("Empty key value in put method");
            }
            values[key] = value;
        }
        public void putAll(ContentValues other)
        {
            if (other.size() == 0)
            {
                throw new SQLiteException("Empty ContentValues in put method");
            }
            foreach(KeyValuePair<string, object?> pair in other.values)
            {
                values[pair.Key] = pair.Value;
            }
        }
        public void putNull(string key)
        {
            values[key] = null;
        }
        public void remove(string key)
        {
            values.Remove(key);
        }
        public int size()
        {
            return values.Count;
        }
        public Hashtable valueSet()
        {
            return new Hashtable(values);
        }
        public virtual string toString()
        {
            return string.Join(", ", values);
        }
    }

    public class SQLiteHelper : SQLiteOpenHelper;

    public class SQLiteDatabase : IDisposable
    {
        private int Version { get; set; } = 0;
        private string Path { get; set; } = string.Empty;
        private SqliteConnection _connection { get; set; }
        private SqliteTransaction? _transaction { get; set; } = null;
        private SqliteConnectionStringBuilder _connectionStringBuilder { get; set; }

        private static readonly string[] separator = new[] { "AND", "OR" };

        public SQLiteDatabase(string path)
        {
            Path = path;
            SQLiteHelper helper = new();

        }

        public void Dispose()
        {
            try
            {
                closeDatabase();
                GC.SuppressFinalize(this);
            }
            finally { }
        }

        public void closeDatabase()
        {
            _connection.Close();
        }

        /// <summary>
        /// Begins a transaction.
        /// </summary>
        public void beginTransaction()
        {
            _transaction = _connection.BeginTransaction();
        }

        /// <summary>
        /// Convenience method for deleting rows in the database.
        /// </summary>
        /// <param name="table"><see cref="string"/>: the table to delete from</param>
        /// <param name="whereClause"><see cref="string"/>: the optional WHERE clause to apply when deleting. Passing null will delete all rows.</param>
        /// <param name="whereArgs"><see cref="Array"/>&lt;<see cref="string"/>&gt;: You may include ?s in the where clause, which will be replaced by the values from whereArgs. The values will be bound as Strings.</param>
        /// <exception cref="SQLiteException"></exception>
        public void delete(string table, string? whereClause, string[]? whereArgs = null)
        {
            // table: drink, whereClause: name = ? and desc = ?, whereArgs: ["latte", "later"]

            if (whereClause is null)
            {
                execSQL($"DELETE FROM {table}");
                return;
            }

            string[] where = whereClause.Split(separator, StringSplitOptions.None).Select(x => x.Trim()).ToArray();

            if (whereArgs is not null)
            {
                if (where.Length != whereArgs.Length)
                {
                    throw new SQLiteException("Length of whereArgs not equals to '?' count");
                }

                if (whereArgs is not null)

                    for (int i = 0; i < whereArgs.Length; i++)
                    {
                        where[i] = where[i].Replace("?", $"'{whereArgs[i]}'");
                    }
            }

            string sql = $"DELETE FROM {table} WHERE {string.Join(" AND ", where)}";
            
            int rowsAffected = execSQL(sql);
            Debug.WriteLine(sql);
        }

        /// <summary>
        /// Deletes a database
        /// </summary>
        /// <param name="filepath"><see cref="string"/> filepath to database</param>
        /// <exception cref="FileNotFoundException"></exception>
        public static void deleteDatabase(string filepath)
        {
            if (!File.Exists(filepath))
            {
                throw new FileNotFoundException("Database file not found");
            }

            File.Delete(filepath);
        }

        /// <summary>
        /// End a transaction.
        /// </summary>
        public void endTransaction()
        {
            _transaction?.Commit();
            _transaction?.Dispose();
            _transaction = null;
        }

        /// <summary>
        /// End a transaction asynchronous.
        /// </summary>
        public void endTransactionAsync()
        {
            _transaction?.CommitAsync();
            _transaction?.DisposeAsync();
            _transaction = null;
        }

        /// <summary>
        /// Execute a single SQL statement that is NOT a SELECT or any other SQL statement that returns data.
        /// </summary>
        /// <param name="sql"><see cref="string"/>: SQL command text</param>
        /// <returns>Count of affected rows</returns>
        public int execSQL(string sql)
        {
            SqliteCommand cmd = new(sql, _connection);
            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Execute a <em>single</em> SQL statement that is <b>NOT</b> a SELECT/INSERT/UPDATE/DELETE.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="bindArgs"><see cref="object"/>: только <see cref="string"/>, <see cref="int"/> и <see cref="double"/> допустимы</param>
        /// <returns>Count of affected rows</returns>
        /// <exception cref="SQLiteException">if the SQL string is invalid</exception>
        public int execSQL(string sql, object[]? bindArgs)
        {
            if (bindArgs is null)
            {
                return execSQL(sql);
            }

            string[] args = sql.Split(',').Select(x => x.Trim()).ToArray();

            foreach (object arg in bindArgs)
            {
                if (arg is not string and not int and not double)
                {
                    throw new SQLiteException("Object in bindArgs is not string nor int nor double");
                }
            }

            for (int i = 0; i < bindArgs.Length; i++)
            {
                args[i] = args[i].Replace("?", bindArgs[i] is string ? $"'{bindArgs[i]}'" : bindArgs[i].ToString());
            }
            return execSQL(string.Join(", ", args));
        }

        /// <summary>
        /// Gets the path to the database file.
        /// </summary>
        /// <returns>Path to the database</returns>
        public string getPath()
        {
            return Path;
        }

        /// <summary>
        /// Gets the database version
        /// </summary>
        /// <returns>Database version</returns>
        public int getVersion()
        {
            return Version;
        }

        /// <summary>
        /// Sets the path to the database file.
        /// </summary>
        /// <param name="path"><see cref="string"/>: path to database</param>
        public void setPath(string path)
        {
            Path = path;
        }

        /// <summary>
        /// Sets the database version
        /// </summary>
        /// <param name="version"><see cref="int"/>: database version above <seealso href="-1"/></param>
        public void setVersion(int version)
        {
            VersionException.ThrowIfNegative(version);
            Version = version;
        }

        /// <summary>
        /// Convenience method for inserting a row into the database.
        /// </summary>
        /// <param name="table"><see cref="string"/>: the table to insert the row into</param>
        /// <param name="nullColumnHack"><see cref="string"/>: optional; may be null. SQL doesn't allow inserting a completely empty row without naming at least one column name. If your provided values is empty, no column names are known and an empty row can't be inserted. If not set to null, the nullColumnHack parameter provides the name of nullable column name to explicitly insert a NULL into in the case where your values is empty.</param>
        /// <param name="values"><see cref="ContentValues"/>: this map contains the initial column values for the row. The keys should be the column names and the values the column values</param>
        /// <returns>The row ID of the newly inserted row, or -1 if an error occurred</returns>
        public long insert(string table, string? nullColumnHack, ContentValues values)
        {
            try
            {
                string sql;
                if (nullColumnHack is null)
                {
                    ContentValues cv = new();
                    foreach (KeyValuePair<string, object?> pair in values.values)
                    {
                        if (pair.Value is string)
                        {
                            cv.values[pair.Key] = $"'{values.values[pair.Key]}'";
                        }
                        else
                        {
                            cv.values[pair.Key] = values.values[pair.Key];
                        }
                    }

                    sql = $"INSERT INTO {table}({string.Join(", ", cv.values.Keys)}) VALUES({string.Join(", ", cv.values.Values)})";
                    Debug.WriteLine(sql);
                    return execSQL(sql);
                }
                else
                {
                    sql = $"INSERT INTO {table}({nullColumnHack}) VALUES(NULL)";
                    return execSQL(sql);
                }
            }
            catch
            {
                return -1;
            }
        }

        public long insertOrThrow(string table, string? nullColumnHack, ContentValues values)
        {
            long result = insert(table, nullColumnHack, values);
            if (result != -1)
            {
                return result;
            }
            throw new SQLiteException("Exception thrown in insertOrThrow method");
        }

        /// <summary>
        /// Checks if the database is opened
        /// </summary>
        /// <returns><seealso href="true"/> if opened else <seealso href="false"/></returns>
        public bool isOpen()
        {
            return _connection.State is ConnectionState.Open;
        }

        public bool needUpgrade(int newVersion)
        {
            return newVersion > Version;
        }

        public SQLiteDatabase openDatabase()
        {
            _connection.Open();
            return this;
        }

        public SQLiteDatabase openOrCreateDatabase(string path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            return openDatabase();
        }

        public DataTable query
        (
            string table,
            string[] columns,
            string selection,
            string[] selectionArgs,
            string groupBy,
            string having,
            string orderBy
        )
        {
            return null;
        }

        public DataTable query
        (
            string table,
            string[] columns,
            string selection,
            string[] selectionArgs,
            string groupBy,
            string having,
            string orderBy,
            string limit
        )
        {
            return null;
        }

        public DataTable query
        (
            bool distinct,
            string table,
            string[] columns,
            string selection,
            string[] selectionArgs,
            string groupBy,
            string having,
            string orderBy,
            string limit
        )
        {
            return null;
        }

        /// <summary>
        /// Runs the provided <paramref name="sql"/> and returns the <see cref="DataTable"/>.
        /// </summary>
        /// <param name="sql"><see cref="string"/>: the SQL query. The SQL string must not be ; terminated</param>
        /// <param name="selectionArgs"><see cref="Array"/>&lt;<see cref="string"/>&gt;: You may include ?s in where clause in the query, which will be replaced by the values from selectionArgs. The values will be bound as Strings.</param>
        /// <returns><see cref="DataTable"/>: result in data table form</returns>
        /// <exception cref="SQLiteException"></exception>
        public DataTable rawQuery(string sql, string[]? selectionArgs)
        {
            if (selectionArgs is null)
            {
                using (SqliteCommand command = new(sql, _connection))
                {
                    using SqliteDataReader reader = command.ExecuteReader();
                    DataTable table = new();
                    table.Load(reader);
                    return table;
                }
            }

            string[] where = sql.Split(separator, StringSplitOptions.None).Select(x => x.Trim()).ToArray();
            
            if (where.Length != selectionArgs.Length)
            {
                throw new SQLiteException("?s length not equals to selectionArgs length");
            }

            for (int i = 0; i < selectionArgs.Length; i++)
            {
                where[i] = where[i].Replace("?", selectionArgs[i]);
            }

            Debug.WriteLine(string.Join(" AND ", where), _connection);

            using (SqliteCommand command = new(string.Join(" AND ", where), _connection))
            {
                SqliteDataReader reader = command.ExecuteReader();
                DataTable table = new();
                table.Load(reader);
                return table;
            }
        }
    }
}
#pragma warning restore IDE1006
