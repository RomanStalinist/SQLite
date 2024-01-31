using System.IO;
using System.Data;
using System.Diagnostics;
using System.Collections;
using Microsoft.Data.Sqlite;
using System.Security.Cryptography.X509Certificates;

#pragma warning disable IDE1006

namespace database.sqlite
{
    public enum SQLiteOpenMode
    {
        OPEN_READWRITECREATE = 0,
        OPEN_READWRITE = 1,
        OPEN_READONLY = 2,
        OPEN_MEMORY = 3
    }

    public class Cursor
    {
        public static int position = -1;
        private static SqliteDataReader _reader = null!;
        private static string _databaseName = string.Empty;
        private static SqliteConnection _connection = null!;
        public static implicit operator Cursor(SqliteDataReader reader) => new(reader);

        public Cursor(SqliteDataReader reader)
        {
            _reader = reader;
        }

        public Cursor(SqliteConnection conn, SqliteDataReader reader)
        {
            _reader = reader;
            _connection = conn;
        }

        public Cursor(SqliteConnection conn, SqliteCommand com)
        {
            _connection = conn;
            _reader = com.ExecuteReader();
        }

        public Cursor(string dbName, SqliteConnection conn, SqliteCommand com)
        {
            _connection = conn;
            _databaseName = dbName;
            _reader = com.ExecuteReader();
        }

        public bool moveToFirst()
        {
            position = 0;
            return true;
        }

        public bool moveToPosition(int offset)
        {
            if (offset <= 0 || offset >= getCount())
                throw new ArgumentOutOfRangeException(nameof(_reader.FieldCount), "Field count out of range");

            position = offset;
            return true;
        }

        public bool moveToLast()
        {
            if (_reader.FieldCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(_reader.FieldCount), "Field count <= 0");

            position = getCount() - 1;
            return true;
        }

        public bool moveToNext()
        {
            position++;
            return read();
        }

        public Cursor MoveToPrevious()
        {
            if (_reader.FieldCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(_reader.FieldCount), "Field count <= 0");

            position--;
            return this;
        }

        public void close() => _reader.Close();

        public int getColumnCount() => _reader.FieldCount;

        public int getColumnIndex(string columnName) => _reader.GetOrdinal(columnName);

        public string getColumnName(int index) => _reader.GetName(index);

        public string[] getColumnNames()
        {
            string[] columnNames = new string[_reader.FieldCount];

            for (int i = 0; i < _reader.FieldCount; i++)
                columnNames[i] = _reader.GetName(i);

            return columnNames;
        }

        public string getDatabase() => _databaseName;

        public byte[] getBlob(int columnIndex) => _reader.GetFieldValue<byte[]>(columnIndex);

        public int getCount()
        {
            int i = 0;
            while (_reader.Read())
                i++;

            return i;
        }

        public double getDouble(int columnIndex) => _reader.GetFieldValue<double>(columnIndex);

        public float getFloat(int columnIndex) => _reader.GetFieldValue<float>(columnIndex);

        public int getInt(int columnIndex) => _reader.GetFieldValue<int>(columnIndex);

        public long getLong(int columnIndex) => _reader.GetFieldValue<long>(columnIndex);

        public short getShort(int columnIndex) => _reader.GetFieldValue<short>(columnIndex);

        public string getString(int columnIndex) => _reader.GetFieldValue<string>(columnIndex);

        public Type getType(int columnIndex) => _reader.GetFieldType(columnIndex);

        public bool isAfterLast() => _reader.IsClosed || _reader.FieldCount <= 0 || position > _reader.FieldCount;

        public bool isBeforeFirst() => _reader.IsClosed || _reader.FieldCount <= 0 || position < 0;

        public bool isClosed() => _reader.IsClosed;

        public bool isFirst() => position == 0;

        public bool isLast() => position == _reader.FieldCount - 1;

        public bool isNull(int columnIndex) => _reader.IsDBNull(columnIndex);

        public void move(int offset) => position += offset;

        public bool read() => !isClosed() && _reader.Read();
    }

    public class SQLiteException : SqliteException
    {
        public int errorCode
        {
            get => errorCode;
            set
            {
                ArgumentOutOfRangeException.ThrowIfLessThan(value, -1, nameof(value));
                errorCode = value;
            }
        }

        public SQLiteOpenMode mode { get; set; } = SQLiteOpenMode.OPEN_READWRITE;

        public CharSequence message { get; set; } = CharSequence.empty;

        public SQLiteException(): base(CharSequence.empty, -1)
        {
            Debug.WriteLine(StackTrace);
        }
        public SQLiteException(CharSequence Message): base(Message, -1)
        {
            message = Message;

            Debug.WriteLine(StackTrace);
        }
        public SQLiteException(CharSequence Message, int ErrorCode) : base(Message, ErrorCode)
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
                throw new VersionException(versions);
        }
        public static void ThrowIfEquals(int oldVersion, int newVersion)
        {
            if (oldVersion == newVersion)
                throw new VersionException(oldVersion, newVersion);
        }
        public static void ThrowIfNotEquals(Versions versions)
        {
            if (versions.oldVersion != versions.newVersion)
                throw new VersionException(versions);
        }
        public static void ThrowIfNotEquals(int oldVersion, int newVersion)
        {
            if (oldVersion != newVersion)
                throw new VersionException(oldVersion, newVersion);
        }
        public static void ThrowIfLess(Versions versions)
        {
            if (versions.oldVersion < versions.newVersion)
                throw new VersionException(versions);
        }
        public static void ThrowIfLess(int oldVersion, int newVersion)
        {
            if (oldVersion < newVersion)
                throw new VersionException(oldVersion, newVersion);
        }
        public static void ThrowIfLessOrEqual(Versions versions)
        {
            if (versions.oldVersion <= versions.newVersion)
                throw new VersionException(versions);
        }
        public static void ThrowIfLessOrEqual(int oldVersion, int newVersion)
        {
            if (oldVersion <= newVersion)
                throw new VersionException(oldVersion, newVersion);
        }
        public static void ThrowIfGreater(Versions versions)
        {
            if (versions.oldVersion > versions.newVersion)
                throw new VersionException(versions);
        }
        public static void ThrowIfGreater(int oldVersion, int newVersion)
        {
            if (oldVersion > newVersion)
                throw new VersionException(oldVersion, newVersion);
        }
        public static void ThrowIfGreaterOrEqual(Versions versions)
        {
            if (versions.oldVersion >= versions.newVersion)
                throw new VersionException(versions);
        }
        public static void ThrowIfGreaterOrEqual(int oldVersion, int newVersion)
        {
            if (oldVersion >= newVersion)
                throw new VersionException(oldVersion, newVersion);
        }
        public static void ThrowIfNegative(int version)
        {
            if (version < 0)
                throw new VersionException(version);
        }
        public static void ThrowIfZero(int version)
        {
            if (version == 0)
                throw new VersionException(version);
        }
        public static void ThrowIfNegativeOrZero(int version)
        {
            if (version <= 0)
                throw new VersionException(version);
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

    public class SQLiteDatabase : IDisposable
    {

        private static int _version { get; set; } = 0;

        private static readonly string[] _separators = ["AND", "OR"];
        private SqliteTransaction? _transaction { get; set; } = null!;
        private static SqliteConnection _connection { get; set; } = null!;
        private static string _connectionString { get; set; } = string.Empty;

        public SQLiteDatabase(CharSequence connectionString)
        {
            _connectionString = connectionString;
            _connection = new(connectionString);
        }

        public SQLiteDatabase(CharSequence databaseName, CharSequence connectionString, int databaseVersion)
        {
            _connectionString = connectionString;
            _connection = new(connectionString);
        }

        public void dispose()
        {
            try
            {
                closeDatabase();
                GC.SuppressFinalize(this);
            }
            finally { }
        }

        public void Dispose() => dispose();

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

            string[] where = whereClause.Split(_separators, StringSplitOptions.None).Select(x => x.Trim()).ToArray();

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
            return _connectionString;
        }

        /// <summary>
        /// Gets the database version
        /// </summary>
        /// <returns>Database version</returns>
        public int getVersion()
        {
            return _version;
        }

        /// <summary>
        /// Sets the path to the database file.
        /// </summary>
        /// <param name="path"><see cref="string"/>: path to database</param>
        public void setPath(string path)
        {
            _connectionString = path;
        }

        /// <summary>
        /// Sets the database version
        /// </summary>
        /// <param name="version"><see cref="int"/>: database version above <seealso href="-1"/></param>
        public void setVersion(int version)
        {
            VersionException.ThrowIfNegative(version);
            _version = version;
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
            return newVersion > _version;
        }

        public SQLiteDatabase openDatabase(SQLiteOpenMode mode)
        {
            string connStr = _connectionString + (mode is SQLiteOpenMode.OPEN_READONLY ? "; Mode=ReadOnly" : mode is SQLiteOpenMode.OPEN_READWRITE or SQLiteOpenMode.OPEN_READWRITECREATE ? "; Mode=ReadWrite" : CharSequence.empty);
            _connection = new(connStr);
            _connection.Open();
            return this;
        }

        public SQLiteDatabase openOrCreateDatabase(string path, SQLiteOpenMode mode)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            return openDatabase(mode);
        }

        public Cursor query
        (
            string table,
            string[] columns,
            string? selection,
            string[]? selectionArgs,
            string? groupBy,
            string? having,
            string? orderBy
        )
        {
            if (selection is not null)
            {
                string[] arr = selection.Split(["AND", "OR"], StringSplitOptions.RemoveEmptyEntries);

                for (int i = 0; i < arr.Length; i++)
                    if (selectionArgs is not null)
                        arr[i] = arr[i].Replace("?", selectionArgs[i]);

                string selectionChanged = string.Join(string.Empty, arr);

                string sql =
                 $@"SELECT {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    WHERE {selectionChanged}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
            else
            {
                string sql =
                 $@"SELECT {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
        }

        public Cursor query
        (
            string table,
            string[] columns,
            string? selection,
            string[]? selectionArgs,
            string? groupBy,
            string? having,
            string? orderBy,
            string? limit
        )
        {
            if (selection is not null)
            {
                string[] arr = selection.Split(["AND", "OR"], StringSplitOptions.RemoveEmptyEntries);

                for (int i = 0; i < arr.Length; i++)
                    if (selectionArgs is not null)
                        arr[i] = arr[i].Replace("?", selectionArgs[i]);

                string selectionChanged = string.Join(string.Empty, arr);

                string sql =
                 $@"SELECT {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    WHERE {selectionChanged}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}
                    {(limit is null ? string.Empty : $" LIMIT {limit}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
            else
            {
                string sql =
                 $@"SELECT {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}
                    {(limit is null ? string.Empty : $" LIMIT {limit}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
        }

        public Cursor query
        (
            bool distinct,
            string table,
            string[] columns,
            string? selection,
            string[]? selectionArgs,
            string? groupBy,
            string? having,
            string? orderBy
        )
        {
            if (selection is not null)
            {
                string[] arr = selection.Split(["AND", "OR"], StringSplitOptions.RemoveEmptyEntries);

                for (int i = 0; i < arr.Length; i++)
                {
                    arr[i] = arr[i].Replace("?", selectionArgs[i]);
                }

                string selectionChanged = string.Join(string.Empty, arr);

                string sql =
                 $@"SELECT{(distinct ? " DISTINCT" : string.Empty)} {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    WHERE {selectionChanged}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
            else
            {
                string sql =
                 $@"SELECT{(distinct ? " DISTINCT" : string.Empty)} {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
        }

        public Cursor query
        (
            bool distinct,
            string table,
            string[] columns,
            string? selection,
            string[]? selectionArgs,
            string? groupBy,
            string? having,
            string? orderBy,
            string? limit
        )
        {
            if (selection is not null)
            {
                string[] arr = selection.Split(["AND", "OR"], StringSplitOptions.RemoveEmptyEntries);

                for (int i = 0; i < arr.Length; i++)
                {
                    arr[i] = arr[i].Replace("?", selectionArgs[i]);
                }

                string selectionChanged = string.Join(string.Empty, arr);

                string sql =
                 $@"SELECT{(distinct ? " DISTINCT" : string.Empty)} {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    WHERE {selectionChanged}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}
                    {(limit is null ? string.Empty : $" LIMIT {limit}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
            else
            {
                string sql =
                 $@"SELECT{(distinct ? " DISTINCT" : string.Empty)} {(columns.Length == 1 && columns[0] == "*" ? columns[0] : string.Join(", ", columns))}
                    FROM {table}
                    {(groupBy is null ? string.Empty : $" GROUP BY {groupBy}")}
                    {(having is null ? string.Empty : $" HAVING {having}")}
                    {(orderBy is null ? string.Empty : $" ORDER BY {orderBy}")}
                    {(limit is null ? string.Empty : $" LIMIT {limit}")}".Trim();

                SqliteCommand com = new(sql, _connection);
                return com.ExecuteReader();
            }
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

            string[] where = sql.Split(_separators, StringSplitOptions.None).Select(x => x.Trim()).ToArray();
            
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
