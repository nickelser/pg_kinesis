package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/jackc/pgx"
	"github.com/nickelser/parselogical"

	"github.com/jpillora/backoff"
	"github.com/pkg/errors"
	"github.com/tevino/abool"
)

const version string = "v0.1.0"
const usage string = `pg_kinesis: replicate output from Postgres' test_decoder plugin to AWS Kinesis

Usage:
  pg_kinesis --source=postgres://user:pass@src.example.com/sourcedb --stream=example_stream --tables=public.users,public.events

Options:
  -h --help             Show this message.
  --slot                Logical replication slot on the source DB. (default: pg_kinesis)
  --source              Source Postgres database URI/DSN. You can also use the libpq-standard PGDATABASE/PGHOST/&c. environment variables (which are tried first; --source overrides the environment).
  --stream              Name of the AWS Kinesis stream to stream to. (required)
  --create              Create the specified replication slot before starting replication.
  --drop                Drop the specified replication slot. Exits upon success.
  -t --table            Table to transfer. Multiple tables can be selected by writing multiple -t switches. Defaults to all tables. The matching semantics are the same as psql (https://www.postgresql.org/docs/current/static/app-psql.html#app-psql-patterns)
  -T --exclude-table    Table to exclude. Defaults to excluding no tables. The matching logic is the same as for -t; -T has higher precedence than -t.
  --retry-initial       If this flag is present, retry the initial connection to the replication slot; useful for high-availability setups where the same pg_kinesis command is run from multiple hosts.
`

// DefaultKeepaliveTimeout is the time before we proactively send a keepalive & status update
const DefaultKeepaliveTimeout = 5 * time.Second

// ReplicationLoopInterval is the time between update checks
const ReplicationLoopInterval = 1 * time.Second

// ReconnectInterval is the time between connection attempts
const ReconnectInterval = 1 * time.Second

// InitialReconnectInterval is the time interval between initial connection attempts (for HA setups)
const InitialReconnectInterval = 5 * time.Second

// StatsInterval is the time between statistics reporting
const StatsInterval = 10 * time.Second

// FlushInterval is the interval between forced Kinesis flushes
const FlushInterval = 1 * time.Second

const (
	maxRecordSize         = 1 << 20 // 1MiB
	maxRequestSize        = 5 << 20 // 5MiB
	maxRecordsPerRequest  = 500
	defaultMaxConnections = 24
)

var stats struct {
	sync.Mutex

	updates        uint64
	inserts        uint64
	deletes        uint64
	skipped        uint64
	putRecords     uint64
	putRecordsTime time.Duration
}

var sigs = make(chan os.Signal, 1)
var restart = make(chan bool, 1)
var shutdown = make(chan bool, 1)
var flush = make(chan bool, 1)
var done = abool.New()
var forceAck = abool.New()

var walLock sync.Mutex
var maxWal uint64
var maxWalSent uint64
var lastStatus time.Time
var lastFlush time.Time

type tableList []*regexp.Regexp

var tables tableList
var excludedTables tableList

var kinesisClient *kinesis.Kinesis

var records []*kinesis.PutRecordsRequestEntry
var lastMsg *pgx.ReplicationMessage

var tablesToStream map[string]bool

var initiallyConnected = false

func logerror(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Fprintln(os.Stdout, file, ":", line, "-", err)
	}
}

func logf(format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Fprintln(os.Stdout, file, ":", line, " ", fmt.Sprintf(format, a...))
}

func logerrf(format string, a ...interface{}) {
	logerror(errors.Errorf(format, a...))
}

func print(a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Fprintln(os.Stdout, file, ":", line, " ", fmt.Sprint(a...))
}

func flushRecords(stream *string) (bool, error) {
	if len(records) == 0 {
		return false, nil
	}

	b := &backoff.Backoff{
		Jitter: true,
	}

	for b.Attempt() < 100 && !done.IsSet() {
		retryDuration := b.Duration()

		startTime := time.Now()
		out, err := kinesisClient.PutRecords(&kinesis.PutRecordsInput{
			StreamName: stream,
			Records:    records,
		})
		elapsed := time.Since(startTime)

		if err != nil {
			logerror(errors.Wrapf(err, "kinesis PutRecords failed; retrying failed records in %s", retryDuration.String()))
			kinesisClient = kinesis.New(session.New(aws.NewConfig())) // refresh the client to get new credentials etc.
			time.Sleep(retryDuration)
		} else if *out.FailedRecordCount > 0 {
			logerrf("%d records failed during Kinesis PutRecords; retrying in %s", *out.FailedRecordCount, retryDuration.String())
			records = failures(records, out.Records)
			time.Sleep(retryDuration)
		} else if *out.FailedRecordCount == 0 {
			stats.putRecords += uint64(len(records))
			stats.putRecordsTime += elapsed
			records = nil
			return true, nil
		}

		if done.IsSet() {
			return false, errors.New("interrupted PutRecords due to shutdown")
		}
	}

	return false, errors.New("failed to put records after many attempts")
}

func putRecord(jsonRecord []byte, tableSchema *string, stream *string) (bool, error) {
	if len(jsonRecord) > maxRecordSize {
		return false, errors.New("replication messages must be less than 1MB in size")
	}

	records = append(records, &kinesis.PutRecordsRequestEntry{
		Data:         jsonRecord,
		PartitionKey: tableSchema,
	})

	if len(records) < maxRecordsPerRequest {
		return false, nil
	}

	return flushRecords(stream)
}

func failures(records []*kinesis.PutRecordsRequestEntry,
	response []*kinesis.PutRecordsResultEntry) (out []*kinesis.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return out
}

func marshalColumnValue(cv *parselogical.ColumnValue) map[string]string {
	return map[string]string{"v": cv.String, "t": cv.Type, "q": strconv.FormatBool(cv.Quoted)}
}

func marshalColumnValuePair(newValue *parselogical.ColumnValue, oldValue *parselogical.ColumnValue) map[string]map[string]string {
	if oldValue != nil && newValue != nil {
		return map[string]map[string]string{
			"old": marshalColumnValue(oldValue),
			"cur": marshalColumnValue(newValue),
		}
	} else if newValue != nil {
		return map[string]map[string]string{
			"cur": marshalColumnValue(newValue),
		}
	} else if oldValue != nil {
		return map[string]map[string]string{
			"old": marshalColumnValue(oldValue),
		}
	}

	return nil
}

func marshalWALToJSON(pr *parselogical.ParseResult, msg *pgx.ReplicationMessage) ([]byte, error) {
	columns := make(map[string]map[string]map[string]string)

	for k, v := range pr.Columns {
		oldV, ok := pr.OldColumns[k]

		if pr.Operation == "DELETE" {
			columns[k] = marshalColumnValuePair(nil, &v)
		} else {
			if ok && v.String != oldV.String {
				columns[k] = marshalColumnValuePair(&v, &oldV)
			} else {
				columns[k] = marshalColumnValuePair(&v, nil)
			}
		}
	}

	lsn := pgx.FormatLSN(msg.WalMessage.WalStart)

	return json.Marshal(struct {
		Lsn       *string                                  `json:"lsn"`
		Table     *string                                  `json:"table"`
		Operation *string                                  `json:"operation"`
		Columns   *map[string]map[string]map[string]string `json:"columns"`
	}{
		Lsn:       &lsn,
		Table:     &pr.Relation,
		Operation: &pr.Operation,
		Columns:   &columns,
	})
}

func handleReplicationMsg(msg *pgx.ReplicationMessage, stream *string) error {
	var err error

	walString := string(msg.WalMessage.WalData)
	pr := parselogical.NewParseResult(walString)
	err = pr.ParsePrelude()

	if err != nil {
		return errors.Wrapf(err, "unable to parse table or operation type of replication message: %s", walString)
	}

	if pr.Operation == "BEGIN" || pr.Operation == "COMMIT" {
		ack(msg)
		return nil
	}

	stats.Lock()
	defer stats.Unlock()

	include, ok := tablesToStream[pr.Relation]

	if !ok {
		include = len(tables) == 0

		for _, tblRegex := range tables {
			if tblRegex.MatchString(pr.Relation) {
				include = true
				break
			}
		}

		for _, tblRegex := range excludedTables {
			if tblRegex.MatchString(pr.Relation) {
				include = false
				break
			}
		}

		tablesToStream[pr.Relation] = include
	}

	if !include {
		stats.skipped++
		ack(msg)
		return nil
	}

	switch pr.Operation {
	case "UPDATE":
		stats.updates++
	case "INSERT":
		stats.inserts++
	case "DELETE":
		stats.deletes++
	}

	err = pr.ParseColumns()

	if err != nil {
		return errors.Wrapf(err, "unable to parse columns of the replication message: %s", walString)
	}

	jsonRecord, err := marshalWALToJSON(pr, msg)

	if err != nil {
		return errors.Wrap(err, "error serializing WAL record into JSON")
	}

	flushed, err := putRecord(jsonRecord, &pr.Relation, stream)

	if err != nil {
		return errors.Wrap(err, "unable to put record into Kinesis")
	}

	lastMsg = msg

	if flushed {
		ack(msg)
	}

	return nil
}

func replicationLoop(replicationMessages chan *pgx.ReplicationMessage, replicationFinished chan error, stream *string) {
	var msg *pgx.ReplicationMessage

	for {
		select {
		case <-restart:
			logerrf("restarting replication loop")
			return
		case <-shutdown:
			logerrf("shutting down replication loop")
			return
		case <-flush:
			flushed, err := flushRecords(stream)

			if err != nil {
				replicationFinished <- err // already wrapped
				return
			}

			if flushed {
				ack(lastMsg)
			}
		case msg = <-replicationMessages:
			err := handleReplicationMsg(msg, stream)

			if err != nil {
				replicationFinished <- err // already wrapped
				return
			}
		}
	}
}

func ack(msg *pgx.ReplicationMessage) {
	walLock.Lock()
	defer walLock.Unlock()

	if msg.WalMessage.WalStart > maxWal {
		maxWal = msg.WalMessage.WalStart
		forceAck.SetTo(true)
	}
}

func sendKeepalive(conn *pgx.ReplicationConn, force bool) error {
	walLock.Lock()
	defer walLock.Unlock()

	if force || forceAck.IsSet() || time.Since(lastStatus) >= DefaultKeepaliveTimeout || maxWal > maxWalSent {
		status, err := pgx.NewStandbyStatus(maxWal)
		if err != nil {
			return err
		}

		err = conn.SendStandbyStatus(status)
		if err != nil {
			return err
		}

		lastStatus = time.Now()
		maxWalSent = maxWal
		forceAck.SetTo(false)
	}

	return nil
}

func connectReplicateLoop(slot *string, sourceConfig pgx.ConnConfig, stream *string) error {
	var err error
	var replErr error

	conn, err := pgx.ReplicationConnect(sourceConfig)
	if err != nil {
		return errors.Wrapf(err, "unable to establish connection to source DB %s/%s", sourceConfig.Host, sourceConfig.Database)
	}
	defer conn.Close()

	err = conn.StartReplication(*slot, 0, -1)
	if err != nil {
		return errors.Wrapf(err, "unable to start replication to slot %s", *slot)
	}

	kinesisClient = kinesis.New(session.New(aws.NewConfig()))

	lastStatus = time.Now()
	initiallyConnected = true

	replicationMessages := make(chan *pgx.ReplicationMessage)
	replicationFinished := make(chan error, 1)

	lastStats := time.Now()
	lastFlush := time.Now()

	go replicationLoop(replicationMessages, replicationFinished, stream)

	logf("replication started at %v starting from LSN %s", lastStatus, pgx.FormatLSN(maxWalSent))

	for !done.IsSet() {
		var message *pgx.ReplicationMessage
		keepaliveRequested := false

		message, err = conn.WaitForReplicationMessage(ReplicationLoopInterval)

		if err != nil {
			if err != pgx.ErrNotificationTimeout {
				return errors.Wrap(err, "waiting for replication message failed")
			}
		}

		// check if the replicating goroutine died
		select {
		case replErr = <-replicationFinished:
		default:
		}

		if replErr != nil {
			logf("stopping replication due to replication goroutine failure")
			return replErr // already wrapped
		}

		if message != nil {
			if message.WalMessage != nil {
				replicationMessages <- message
			} else if message.ServerHeartbeat != nil {
				keepaliveRequested = message.ServerHeartbeat.ReplyRequested == 1
			}
		}

		err = sendKeepalive(conn, keepaliveRequested)
		if err != nil {
			return errors.Wrap(err, "unable to send keepalive")
		}

		sinceLastFlush := time.Since(lastFlush)

		if sinceLastFlush >= FlushInterval {
			flush <- true
			lastFlush = time.Now()
		}

		sinceLastStats := time.Since(lastStats)
		if sinceLastStats >= StatsInterval {
			stats.Lock()
			timePerInsert := float64(0)
			if float64(stats.putRecordsTime) > 0 {
				timePerInsert = (float64(stats.putRecordsTime) / float64(time.Millisecond)) / float64(stats.putRecords)
			}
			logf("inserts=%d (%.1f/s) updates=%d (%.1f/s) deletes=%d (%.1f/s) skipped=%d (%.1f/s) putrecords=%d (%.1f/s, %.0fms/record, %.1fs total) lsn=%s",
				stats.inserts, float64(stats.inserts)/sinceLastStats.Seconds(),
				stats.updates, float64(stats.updates)/sinceLastStats.Seconds(),
				stats.deletes, float64(stats.deletes)/sinceLastStats.Seconds(),
				stats.skipped, float64(stats.skipped)/sinceLastStats.Seconds(),
				stats.putRecords, float64(stats.putRecords)/sinceLastStats.Seconds(), timePerInsert, float64(stats.putRecordsTime)/float64(time.Second),
				pgx.FormatLSN(maxWalSent))
			stats.inserts = 0
			stats.updates = 0
			stats.deletes = 0
			stats.skipped = 0
			stats.putRecords = 0
			stats.putRecordsTime = 0
			stats.Unlock()
			lastStats = time.Now()
		}
	}

	// if we are closing down, send a last keepalive before closing the connection
	// we can ignore the error safely
	sendKeepalive(conn, true)

	return nil
}

func createReplicationSlot(slot *string, sourceConfig pgx.ConnConfig) error {
	conn, err := pgx.ReplicationConnect(sourceConfig)
	if err != nil {
		return errors.Wrapf(err, "unable to establish connection to source DB %s/%s", sourceConfig.Host, sourceConfig.Database)
	}
	defer conn.Close()

	err = conn.CreateReplicationSlot(*slot, "test_decoding")
	if err != nil {
		return errors.Wrapf(err, "unable to create slot %s", *slot)
	}

	logf("created replication slot %s", *slot)
	return nil
}

func dropReplicationSlot(slot *string, sourceConfig pgx.ConnConfig) error {
	conn, err := pgx.ReplicationConnect(sourceConfig)
	if err != nil {
		return errors.Wrapf(err, "unable to establish connection to source DB %s/%s", sourceConfig.Host, sourceConfig.Database)
	}
	defer conn.Close()

	err = conn.DropReplicationSlot(*slot)
	if err != nil {
		return errors.Wrapf(err, "unable to drop slot %s", *slot)
	}

	logf("dropped replication slot %s", *slot)
	return nil
}

func createTableRegex(str string) *regexp.Regexp {
	tbl := strings.Replace(str, ".", "\\.", -1)
	tbl = strings.Replace(tbl, "?", ".", -1)
	tbl = strings.Replace(tbl, "*", ".*", -1)
	tbl = strings.Replace(tbl, "$", "\\$", -1)
	return regexp.MustCompile(tbl)
}

func (tl *tableList) Set(value string) error {
	*tl = append(*tl, createTableRegex(value))
	return nil
}

func (tl *tableList) String() string {
	return fmt.Sprint(*tl)
}

func signalHandler() {
	sig := <-sigs
	logerrf("received signal: %s, shutting down", sig)

	done.SetTo(true)

	// non-blocking send to shutdown
	select {
	case shutdown <- true:
	default:
	}
}

func main() {
	var err error
	var sourceConfig pgx.ConnConfig

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usage)
	}

	sourceURI := flag.String("source", "", "")
	create := flag.Bool("create", false, "")
	drop := flag.Bool("drop", false, "")
	retryInitial := flag.Bool("retry-initial", false, "")
	slot := flag.String("slot", "pg_kinesis", "")
	stream := flag.String("stream", "", "")
	flag.Var(&tables, "table", "-t")
	flag.Var(&excludedTables, "excluded-table", "-T")

	flag.Parse()

	if *sourceURI != "" {
		sourceConfig, err = pgx.ParseConnectionString(*sourceURI)

		if err != nil {
			logerror(errors.Wrapf(err, "unable to parse source DB URI '%s'", *sourceURI))
			os.Exit(1)
		}
	} else {
		logf("reading target DB configuration from shell environment")
		sourceConfig, err = pgx.ParseEnvLibpq()

		if err != nil {
			logerror(errors.Wrapf(err, "unable to parse environment, and source not specified"))
			fmt.Fprintf(os.Stderr, usage)
			os.Exit(1)
		}
	}

	if *slot == "" {
		logerror(errors.New("blank slot; please specify slot with --slot"))
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	if *slot == "" {
		logerror(errors.New("blank stream; please specify slot with --stream"))
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	if *create && *drop {
		logerror(errors.New("specify one of create or drop, not both"))
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	if *create {
		logerror(createReplicationSlot(slot, sourceConfig))
	}

	if *drop {
		logerror(dropReplicationSlot(slot, sourceConfig))
		os.Exit(0)
	}

	tablesToStream = make(map[string]bool)

	go signalHandler()
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for !done.IsSet() {
		err := connectReplicateLoop(slot, sourceConfig, stream)
		logerror(err)

		if !initiallyConnected && !*retryInitial {
			logerrf("failed to connect initially, exiting; if you wish to retry on the initial connection (for a HA setup), set --retry-initial")
			os.Exit(1)
		}

		if !done.IsSet() {
			// non-blocking send to restart
			select {
			case restart <- true:
			default:
			}

			if initiallyConnected {
				time.Sleep(ReconnectInterval)
			} else {
				time.Sleep(InitialReconnectInterval)
			}
		}
	}
}
