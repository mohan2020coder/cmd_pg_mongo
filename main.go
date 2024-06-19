package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config struct to hold database configuration
type Config struct {
	Postgres struct {
		Host      string   `mapstructure:"host"`
		Port      int      `mapstructure:"port"`
		Database  string   `mapstructure:"database"`
		User      string   `mapstructure:"user"`
		Password  string   `mapstructure:"password"`
		Tables    []string `mapstructure:"tables"`
		AllTables bool     `mapstructure:"all_tables"`
	} `mapstructure:"postgres"`

	MongoDB struct {
		URI      string `mapstructure:"uri"`
		Database string `mapstructure:"database"`
	} `mapstructure:"mongodb"`
}

func main() {
	// Parse command-line arguments
	configFile := flag.String("config", "config.yml", "path to the config file")
	flag.Parse()

	// Load configuration from the specified file or default config.yml using viper
	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Error loading configuration: %v\n", err)
	}

	// Connect to PostgreSQL
	pgConn, err := connectToPostgreSQL(config)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL: %v\n", err)
	}
	defer pgConn.Close()

	// Connect to MongoDB
	mongoClient, err := connectToMongoDB(config)
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v\n", err)
	}
	defer mongoClient.Disconnect(context.Background())

	if config.Postgres.AllTables {
		// Fetch all table names from PostgreSQL
		tables, err := getAllPostgresTables(pgConn, config.Postgres.Database)
		if err != nil {
			log.Fatalf("Error fetching table names from PostgreSQL: %v\n", err)
		}
		config.Postgres.Tables = tables
	}

	// Fetch data from PostgreSQL and insert into MongoDB
	for _, table := range config.Postgres.Tables {
		err = fetchDataFromPostgresAndInsertToMongo(pgConn, mongoClient, table, config.MongoDB.Database, table)
		if err != nil {
			log.Fatalf("Error transferring data from table %s: %v\n", table, err)
		}
		fmt.Printf("Data transfer from PostgreSQL table %s to MongoDB completed successfully.\n", table)
	}
}

// loadConfig reads the config file and parses it into a Config struct
func loadConfig(filename string) (Config, error) {
	var config Config

	viper.SetConfigFile(filename)
	if err := viper.ReadInConfig(); err != nil {
		return config, fmt.Errorf("failed to read config file: %v", err)
	}

	if err := viper.Unmarshal(&config); err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	return config, nil
}

// connectToPostgreSQL establishes a connection to PostgreSQL
func connectToPostgreSQL(pgConfig Config) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s pool_max_conns=10",
		pgConfig.Postgres.Host, pgConfig.Postgres.Port, pgConfig.Postgres.Database, pgConfig.Postgres.User, pgConfig.Postgres.Password)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

// connectToMongoDB establishes a connection to MongoDB
func connectToMongoDB(mongoConfig Config) (*mongo.Client, error) {
	ctx := context.Background()

	clientOptions := options.Client().ApplyURI(mongoConfig.MongoDB.URI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// getAllPostgresTables retrieves all table names from the PostgreSQL database
func getAllPostgresTables(pgConn *pgxpool.Pool, databaseName string) ([]string, error) {
	ctx := context.Background()

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
	`

	rows, err := pgConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying PostgreSQL for table names: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error scanning table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table names: %v", err)
	}

	return tables, nil
}

// fetchDataFromPostgresAndInsertToMongo retrieves data from PostgreSQL and inserts it into MongoDB
func fetchDataFromPostgresAndInsertToMongo(pgConn *pgxpool.Pool, mongoClient *mongo.Client, pgTableName, mongoDBName, mongoCollectionName string) error {
	ctx := context.Background()

	// PostgreSQL query
	rows, err := pgConn.Query(ctx, fmt.Sprintf("SELECT * FROM %s", pgTableName))
	if err != nil {
		return fmt.Errorf("error querying PostgreSQL: %v", err)
	}
	defer rows.Close()

	// MongoDB collection
	mongoCollection := mongoClient.Database(mongoDBName).Collection(mongoCollectionName)

	// Iterate through PostgreSQL rows and insert into MongoDB
	for rows.Next() {
		// Get column names
		fields := rows.FieldDescriptions()
		columnValues := make([]interface{}, len(fields))
		columnPointers := make([]interface{}, len(fields))

		// Prepare a map to store values for dynamic document creation
		document := bson.D{}

		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		// Scan row into the document
		if err := rows.Scan(columnPointers...); err != nil {
			return fmt.Errorf("error scanning PostgreSQL row: %v", err)
		}

		// Populate document dynamically
		for i, field := range fields {
			document = append(document, bson.E{Key: string(field.Name), Value: columnValues[i]})
		}

		// Insert document into MongoDB
		_, err := mongoCollection.InsertOne(ctx, document)
		if err != nil {
			return fmt.Errorf("error inserting document into MongoDB: %v", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating PostgreSQL rows: %v", err)
	}

	return nil
}
