namespace database.sqlite
{
    public enum UpdateMode { Upgrade, Downgrade };

    public abstract class SQLiteOpenHelper : object, IDisposable
    {
        private int _databaseVersion { get; set; }

        public int DATABASE_VERSION {
            get => _databaseVersion;
            set
            {
                VersionException.ThrowIfNegative(value);

                if (DATABASE_VERSION < value)
                {
                    onDowngrade(_db, DATABASE_VERSION, value);
                }
                else if (DATABASE_VERSION > value)
                {
                    onUpgrade(_db, DATABASE_VERSION, value);
                }
                onOpen(_db);
            }
        }
        public string DATABASE_NAME;
        public SQLiteOpenMode DATABASE_MODE = SQLiteOpenMode.OPEN_READWRITE;
        public const string DOWNGRADE_UNAVAILABLE = "Downgrade unavailable for this database";

        private SQLiteDatabase _db { get; set; }

        public SQLiteOpenHelper(string databaseName, string connectionString, int databaseVersion)
        {
            VersionException.ThrowIfNegative(databaseVersion);
            _db = new SQLiteDatabase(connectionString);
            DATABASE_NAME = databaseName;
            DATABASE_VERSION = databaseVersion;

            if (!System.IO.File.Exists(connectionString))
                onCreate(_db);

            if (DATABASE_VERSION < databaseVersion)
                onUpgrade(_db, DATABASE_VERSION, databaseVersion);

            else if (DATABASE_VERSION > databaseVersion)
                onDowngrade(_db, DATABASE_VERSION, databaseVersion);

            onOpen(_db);
        }

        public void dispose()
        {
            try
            {
                onClose(_db);
                GC.SuppressFinalize(this);
            }
            finally { }
        }

        public void Dispose() => dispose();

        public SQLiteDatabase getReadableDatabase()
        {
            DATABASE_MODE = SQLiteOpenMode.OPEN_READONLY;
            return _db.openDatabase(DATABASE_MODE);
        }
        public SQLiteDatabase getWritableDatabase()
        {
            DATABASE_MODE = SQLiteOpenMode.OPEN_READWRITE;
            return _db.openDatabase(DATABASE_MODE);
        }

        protected abstract void onCreate(SQLiteDatabase db);
        protected abstract void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion);
        protected virtual void onOpen(SQLiteDatabase db) => db.openDatabase(DATABASE_MODE);
        protected virtual void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) => throw new NotImplementedException(DOWNGRADE_UNAVAILABLE);
        protected virtual void onClose(SQLiteDatabase db) => db.closeDatabase();
    }
}
