<?php namespace Jenssegers\Mongodb;

use Jenssegers\Mongodb\Collection;
use Jenssegers\Mongodb\Query\Builder as QueryBuilder;
use MongoClient;

class Connection extends \Illuminate\Database\Connection {

    /**
     * The MongoDB database handler.
     *
     * @var MongoDB
     */
    protected $db;

    /**
     * The MongoClient connection handler.
     *
     * @var MongoClient
     */
    protected $connection;

    /**
     * Number of microseconds it took to connect to MongoDB
     * @var float
     */
    public $timeItTookToConnect = 0;

    /**
     * Create a new database connection instance.
     *
     * @param  array   $config
     * @return void
     */
    public function __construct(array $config)
    {
        /**
         * Glue mongoDB options
         */
        $speedOptions  = [

            // If the server is down, don't wait forever
            "connectTimeoutMS"   => 1500,

            // When the server goes down in the middle of operation, don't wait forever
            "socketTimeoutMS"    => 5000,

            "w"                  => "majority",

            // Don't wait forever for majority write acknowledgment
            "wtimeout"           => 500,

            // When the primary goes down, allow reading from secondaries
            "readPreference"     => MongoClient::RP_PRIMARY_PREFERRED,

            //// When the primary is down, prioritize reading from our local datacenter
            //// If that datacenter is down too, fallback to any server available
            //"readPreferenceTags" => array("dc:is", ""),
        ];

        //MongoLog::setLevel(MongoLog::ALL);
        //MongoLog::setModule(MongoLog::ALL);

        $this->config = $config;

        // Build the connection string
        $dsn = $this->getDsn($config);

        // You can pass options directly to the MogoClient constructor
        $options = array_get($config, 'options', array());

        $options = array_merge($options, $speedOptions);

        $before = microtime(true);
        // Create the connection
        $this->connection = $this->createConnection($dsn, $config, $options);
        $after = microtime(true);

        $this->timeItTookToConnect = ($after - $before);

        // Select database
        $this->db = $this->connection->{$config['database']};
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param  string  $collection
     * @return QueryBuilder
     */
    public function collection($collection)
    {
        $query = new QueryBuilder($this);

        return $query->from($collection);
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param  string  $table
     * @return QueryBuilder
     */
    public function table($table)
    {
        return $this->collection($table);
    }

    /**
     * Get a MongoDB collection.
     *
     * @param  string   $name
     * @return MongoDB
     */
    public function getCollection($name)
    {
        return new Collection($this, $this->db->selectCollection($name));
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return Schema\Builder
     */
    public function getSchemaBuilder()
    {
        return new Schema\Builder($this);
    }

    /**
     * Get the MongoDB database object.
     *
     * @return  MongoDB
     */
    public function getMongoDB()
    {
        return $this->db;
    }

    /**
     * return MongoClient object
     *
     * @return MongoClient
     */
    public function getMongoClient()
    {
        return $this->connection;
    }

    /**
     * Create a new MongoClient connection.
     *
     * @param  string  $dsn
     * @param  array   $config
     * @param  array   $options
     * @return MongoClient
     */
    protected function createConnection($dsn, array $config, array $options)
    {
        // Add credentials as options, this makes sure the connection will not fail if
        // the username or password contains strange characters.
        if (isset($config['username']) && $config['username'])
        {
            $options['username'] = $config['username'];
        }

        if (isset($config['password']) && $config['password'])
        {
            $options['password'] = $config['password'];
        }

        $client = new MongoClient($dsn, $options);

        return $client;
        //return new MongoClient($dsn, $options);
    }

    /**
     * Disconnect from the underlying MongoClient connection.
     *
     * @return void
     */
    public function disconnect()
    {
        $this->connection->close();
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param  array   $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        // First we will create the basic DSN setup as well as the port if it is in
        // in the configuration options. This will give us the basic DSN we will
        // need to establish the MongoClient and return them back for use.
        extract($config);

        /**
         * Glue added - checks if connection_string is present, if so it uses that instead
         */
        if(isset($config['connection_string']))
        {
            return $config['connection_string'];
        }

        // Treat host option as array of hosts
        $hosts = is_array($config['host']) ? $config['host'] : array($config['host']);

        // Add ports to hosts
        foreach ($hosts as &$host)
        {
            if (isset($config['port']))
            {
                $host = "{$host}:{$port}";
            }
        }

        // The database name needs to be in the connection string, otherwise it will
        // authenticate to the admin database, which may result in permission errors.
        return "mongodb://" . implode(',', $hosts) . "/{$database}";
    }

    /**
     * Get the elapsed time since a given starting point.
     *
     * @param  int    $start
     * @return float
     */
    public function getElapsedTime($start)
    {
        return parent::getElapsedTime($start);
    }

    /**
    * Get the PDO driver name.
    *
    * @return string
    */
    public function getDriverName()
    {
        return 'mongodb';
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param  string  $method
     * @param  array   $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array(array($this->db, $method), $parameters);
    }

}
