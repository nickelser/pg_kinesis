package main

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/ory-am/dockertest.v3"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
	_ "github.com/lib/pq"
	"encoding/json"
)

var aws_region = "us-east-1"
var aws_access_key_id = "FAKE"
var aws_secret_access_key = "FAKE"

var host = "localhost"
var slotName = "pg_kinesis"

var kinesisImage = "vsouza/kinesis-local"
var kinesisImageTag = "latest"
var kinesisStream = "pgkinesis"
var kinesisShards = 3

var dbImage = "postgres"
var dbImageTag = "9.6"
var dbUser = "pgkinesis"
var dbPass = "pgkinesis"
var dbName = "pgkinesis"
var dbPort = "5432"

var testDb *sql.DB
var testKinesisClient *kinesis.Kinesis

var dbConnString string
var dockerPool *dockertest.Pool
var kinesisContainer *dockertest.Resource
var dbContainer *dockertest.Resource

// ** Helper Functions **

// Simple helper function to give the user some information about the Docker containers we start
// up on their behalf.
func logContainerInfo(container *dockertest.Resource) {
	id    := container.Container.ID
	image := container.Container.Name
	ports := container.Container.Config.ExposedPorts
	log.Printf("Started up %s (%s), listening on %s", image, id, ports)
}

// Sets up all of the backend resources required to run live integration tests.
//
// Spins up a Postgres and "Local" Kinesis container. Stores access information to both of these
// via global variables above. The Kinesis object is primarily used so that we can create the
// streams and inspect the data we put there. The Database object is used to create database
// changes that we expect to see in Kinesis.
//
func setUp() error {
	log.Println("Starting up Docker resources")

	var err error

	// Set up our local Docker connection dockerPool
	dockerPool, err = dockertest.NewPool("")
	if err != nil { return err }

	// Start up Postgres - customize it by passing in our own entrypoint directory that tweaks
	// the Postgres configuration to enable wal replicatin.
	_path, _ := filepath.Abs("resources/docker-entrypoint-initdb.d")
	dbContainer, err = dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: dbImage,
		Tag: dbImageTag,
		Mounts: []string{_path + ":/docker-entrypoint-initdb.d"},
		Env: []string{
			"POSTGRES_PASSWORD=" + dbPass,
			"POSTGRES_USER=" + dbUser,
			"POSTGRES_DB=" + dbName,
			"POSTGRES_PORT=" + dbPort,
		},
	})
	if err != nil { return err }

	// Wait for Docker to tell us the container is up, and then create a DB connection
	err = dockerPool.Retry(func() error {
		var err error

		// Stored globally so that we can pass this into the mainLoop() for testing
		dbConnString = fmt.Sprintf("postgres://" + dbUser + ":" + dbPass + "@" + host + ":%s/%s" +
			"?sslmode=disable", dbContainer.GetPort("5432/tcp"), dbName)

		testDb, err = sql.Open("postgres", dbConnString)
		if err != nil { return err }

		// Ensures that later when we call pg_kinesis,
		// which has no arguments and relies on the LibPQ os.GetEnv() function,
		// that it knows how to connect to our Test DB.
		os.Setenv("PGHOST", host)
		os.Setenv("PGUSER", dbUser)
		os.Setenv("PGPASSWORD", dbPass)
		os.Setenv("PGPORT", string(dbContainer.GetPort("5432/tcp")))

		return testDb.Ping()
	});
	if err != nil { return err }

	// Start up Fake Kinesis - note, for some strange reason the container requires that we pass
	// it at least one option, or it won't start up. Even though this option claims to have a
	// default value, this seems to work well enough.
	kinesisOptions := &dockertest.RunOptions{
		Repository: kinesisImage,
		Tag: kinesisImageTag,
		Cmd: []string{"--createStreamMS 0"},
	}
	kinesisContainer, err = dockerPool.RunWithOptions(kinesisOptions)
	if err != nil { return err }

	// Wait for Docker to tell us the container is up, and then create the test-stream in the
	// local Kinesis service
	err = dockerPool.Retry(func() error {
		var err error

		// Fake out the AWS Region Environment Variable thats required
		os.Setenv("AWS_REGION", aws_region)

		// Generate our fake Kinesis Amazon client - this will be used to prepare the kinesis streams for testing.
		testKinesisClient = kinesis.New(session.New(aws.NewConfig().
			WithRegion(aws_region).
			WithCredentials(credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, "")).
			WithEndpoint("http://" + host + ":" + kinesisContainer.GetPort("4567/tcp")).
			WithDisableSSL(true),
		))

		// In the main package, set our KinesisClient object with this one we just created.
		kinesisClient = testKinesisClient

		// Create our streams - no need for the data to come back..
		_, err = testKinesisClient.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(int64(kinesisShards)),
			StreamName: aws.String(kinesisStream),
		})
		return err
	});
	if err != nil { return err }

	// Just some debug info
	logContainerInfo(dbContainer);
	logContainerInfo(kinesisContainer);
	return nil
}

// Ensure that the Docker resources are torn down after any successful test. Also
// called aggressively during our setUp process just in case that fails.
func tearDown() {
	log.Println("Tearing down resources")
	if dbContainer != nil { dockerPool.Purge(dbContainer) }
	if kinesisContainer != nil { dockerPool.Purge(kinesisContainer) }
}


// Maybe move this into another test file?
type TestUser struct{
	id int
	age int
	first_name string
	last_name string
	email string
}
func createTestDatabase() error {
	var tableCreateStatement = `
		CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		age INT,
		first_name TEXT,
		last_name TEXT,
		email TEXT UNIQUE NOT NULL
		);`
	_, err := testDb.Exec(tableCreateStatement)
	if err != nil {
		return errors.Wrapf(err, "Test Failure - Unable to create table")
	}

	return nil
}
func createTestUser(age int, firstName string, lastName string,
	email string) error {

	var err error

	insertStatement := `
		INSERT INTO users
		(age, first_name, last_name, email)
		VALUES($1, $2, $3, $4)
		RETURNING id`

	_, err = testDb.Exec(insertStatement, age, firstName, lastName, email)
	if err != nil {
		return errors.Wrapf(err, "Test Failure - Unable create user")
	}

	return nil
}

// Function to connect into Kinesis and pull all the data off of it
func getKinesisData() ([]*kinesis.Record, error) {
	var records []*kinesis.Record

	// Get our stream description back from Kinesis...
	stream, err := testKinesisClient.DescribeStream(
		&kinesis.DescribeStreamInput{StreamName: &kinesisStream})
	if err != nil {
		return nil, errors.Wrap(err, "Unable to describe Kinesis Streams")
	}

	// Get our list of shards. For each shard, we're going to get all the data.
	// We walk through each shard in a row, and return all the data in that order.
	// The pattern looks like this:
	//
	//  Shard1: RecordA,RecordB,RecordC
	//  Shard2: Record1,Record2,Record3
	//
	// Turns into:
	//  [RecordA, RecordB, RecordC, Record1, Record2, Record3]
	//
	for _, shard := range stream.StreamDescription.Shards {
		var shardIteratorType = "TRIM_HORIZON"
		var limit int64 = 10000

		// Get a fresh shard iterator for this stream
		it, err := testKinesisClient.GetShardIterator(&kinesis.GetShardIteratorInput{
			ShardId: shard.ShardId,
			ShardIteratorType: &shardIteratorType,
			StreamName: &kinesisStream,
		})
		if err != nil {
			return nil, errors.Wrap(err, "Unable to get our Kinesis Shard Iterator")
		}

		// Get the records off that shard
		req, err := testKinesisClient.GetRecords(&kinesis.GetRecordsInput{
			Limit: &limit,
			ShardIterator: it.ShardIterator,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to get our Kinesis Records from %s", shard)
		}

		// Debug
		log.Printf("%s has %d records", *shard.ShardId, len(req.Records))

		for _, rec := range req.Records {
			records = append(records, rec)
		}
	}

	return records, nil
}

// Overrides the main test runner and wraps it with the setUp() and tearDown() function calls.
func TestMain(m *testing.M) {

	// Run the setUp - if it fails for any reason, clean up after ourselves.
	err := setUp()
	if err != nil {
		tearDown()
		log.Fatalf("Could not start resource: %s", err)
	}

	// Run our tests and save an exit code.
	code := m.Run()
	tearDown()

	// Clean up and exit!
	os.Exit(code)
}

// ** Tests Begin Here **


// Simple test to validate that creating our replication slot from scratch works
func TestCreateReplicationSlot(t *testing.T) {
	assert := assert.New(t)

	// First, set main.done to False. This will trick the app into stopping before it tries to
	// startup any goroutines.
	done.SetTo(true)

	// Call out to the mainLoop - this should exit back right away,
	// so to keep the test simple we don't do this in the background.
	sourceConfig, err := pgx.ParseConnectionString(dbConnString)
	mainLoop(slotName, false, true, kinesisStream, true, sourceConfig)

	// Verify that the encoding type of the slot is test_decoding, as expected
	var expected_plugin = "test_decoding"
	var actual_plugin string

	err = testDb.QueryRow(
		"SELECT plugin FROM pg_replication_slots WHERE slot_name = '" + slotName +"';").
		Scan(&actual_plugin)
	if err != nil { t.Errorf("Error: %s", err) }

	assert.Equal(actual_plugin, expected_plugin, "Replication slot type should match")
}

// Full test suite here
//
// Creates a new pg_kinesis mainLoop() against a live database, inserts a thousand records,
// streams the records to Kinesis,
// and finally picks the records up from Kinesis and validates the content of each and every one
// of them.
//
func TestMainLoop(t *testing.T) {
	assert := assert.New(t)

	// How many records should we INSERT Into the database?
	var testRecordCount = 1000

	var err error

	// Will capture the exit-status of pg_kinesis when we gracefully stop it
	resCh := make(chan error)

	// Will be used to signal that the initiallyConnected bool has been set by pg_kinesis.
	// This ensures that we don't try to write anything to the DB until after pg_kinesis is
	// connected.
	connectedCh := make(chan bool)

	// Ensure we are going to run the full mainLoop()
	done.SetTo(false)

	// Call out to the main loop - in the background.
	// Output is pumped into resCh for validation later.
	sourceConfig, _ := pgx.ParseConnectionString(dbConnString)
	go func(ch chan error) {
		err := mainLoop(slotName, false, false, kinesisStream, false, sourceConfig)
		ch <- err
	}(resCh)

	// In the background, check for the initiallyConnected variable to flip to true. Once it has,
	// we know that pg_kinesis is connected up to postgres properly and we can begin writing to
	// the database.
	go func(ch chan bool) {
		for !initiallyConnected {
			time.Sleep(1 * time.Second)
		}
		ch <- true
	}(connectedCh)

	// Block - either for pg_kinesis to exit (which would be unexpected),
	// for our initially connected check to pass (thats the hope),
	// or finally for a timeout (after 10s) which indicates something went wrong.
	select {
	case <- resCh:
		t.Error("Exited unexpectedly")
	case <- connectedCh:
		log.Printf("MainLoop connected")
	case <- time.After(10 * time.Second):
		t.Error("Exited unexpectedly with timeout")
	}

	// Insert data into the database
	err = createTestDatabase()
	assert.Nil(err)

	// Create test users
	for i := 1; i <= testRecordCount; i++ {
		err = createTestUser(10+i, "Bob"+string(i), "Barker", "bob"+string(i)+"@test.com")
		assert.Nil(err)
	}

	// TEMP - need to wait until records have been flushed
	time.Sleep(3 * time.Second)

	// Get our data from the Kinesis stream
	records, err := getKinesisData()
	assert.Nil(err)

	// Verify that we got all the records back
	assert.Equal(testRecordCount, len(records))

	// Verify that each record is correct.
	for _, record := range records {
		// Test that the PartitionKey was set correctly. Note, this currently means that if you
		// have a heavy-write volume on a single table, you will be unable to scale past 1 shard
		// for that table. This should be refactored ASAP to be configurable.
		assert.EqualValues("public.users", string(*record.PartitionKey))

		// Deserialize the data back into jsonWalEntry objects so we can validate their content
		//
		// The data in Kinesis is a []byte array that can be decoded into a single JSON line.
		// The JSON looks like this:
		//   {
		//     "time": "2018-05-04T10:31:44-07:00",
		//     "lsn": "0/1514C80",
		//     "table": "public.users",
		//     "operation": "INSERT",
		//     "columns": {
		//       "age": {
		//          "new": {
		//            "q":"false",
		//            "t":"integer"
		//            "v":"15"
		//           }
		//       },
		//       "email": {
		//         "new": {
		//           "q": "true",
		//           "t": "text"
		//           "v": "bob\u0005@test.com"
		//          }
		//        },
		//        "first_name": {
		//          "new": {
		//            "q": "true",
		//            "t":"text",
		//            "v":"Bob\u0005"
		//          }
		//        },
		//        "id": {
		//          "new": {
		//            "q": "false",
		//            "t": "integer",
		//            "v": "5"
		//          }
		//        },
		//        "last_name": {
		//          "new": {
		//            "q": "true",
		//            "t": "text",
		//            "v": "Barker"
		//          }
		//        }
		//      }
		//    }
		var entry *jsonWalEntry
		err := json.Unmarshal(record.Data, &entry)
		assert.Nil(err)
		assert.EqualValues("public.users", string(*entry.Table))
		assert.EqualValues("INSERT", string(*entry.Operation))
	}

	// Final teardown!
	done.SetTo(true)
	select {
	case <-resCh:
	case <- time.After(10 * time.Second):
		t.Error("Teardown took longer than 10s!")
	}
}

