package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/jackc/pgx"
	"github.com/nickelser/parselogical"

	"github.com/dustin/go-humanize"
	"github.com/jpillora/backoff"
	"github.com/pkg/errors"
	"github.com/tevino/abool"
)

const version string = "v0.6.0"
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
  --version             Display the current pg_kinesis version and exit.
`

// DefaultKeepaliveTimeout is the time before we proactively send a keepalive & status update
const DefaultKeepaliveTimeout = 4 * time.Second

// ReplicationLoopInterval is the time between update checks
const ReplicationLoopInterval = 1 * time.Second

// ReconnectInterval is the time between connection attempts
const ReconnectInterval = 1 * time.Second

// InitialReconnectInterval is the time interval between initial connection attempts (for HA setups)
const InitialReconnectInterval = 5 * time.Second

// StatsInterval is the time between statistics reporting
const StatsInterval = 10 * time.Second

// FlushInterval is the interval between forced Kinesis flushes
const FlushInterval = 100 * time.Millisecond

const (
	maxRecordSize        = 1 << 20 // 1MiB
	maxRequestSize       = 5 << 20 // 5MiB
	maxRecordsPerRequest = 500
)

const maxSenders = 16
const maxBacklog = 10000 // per channel

var stats struct {
	updates        uint64
	inserts        uint64
	deletes        uint64
	skipped        uint64
	putRecords     uint64
	putRecordsTime uint64
	lag            uint64
}

type putRecordEntry struct {
	stream *string
	msg    *pgx.WalMessage
	pr     *parselogical.ParseResult
	skip   bool
	json   []byte
}

var sigs = make(chan os.Signal, 1)
var restart = make(chan bool, 1)
var shutdown = make(chan bool, 1)
var done = abool.New()

var messagesToStream map[int]chan *putRecordEntry
var tableToInternalChan map[string]int
var maxAckWalPerChan []uint64

var curTableChanIdx int
var pks sync.Map
var maxWal uint64
var maxRecvWal uint64
var latestKinesisSequenceNumber string
var maxWalSent uint64
var lastStatus time.Time

type tableList []*regexp.Regexp

var tables tableList
var excludedTables tableList

var tablesToStream map[string]bool

var initiallyConnected = false

func logerror(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Fprintln(os.Stdout, file, ":", line, "-", err)
	}
}

func logf(format string, a ...interface{}) {
	fmt.Fprintln(os.Stdout, time.Now().Format(time.RFC3339), fmt.Sprintf(format, a...))
}

func logerrf(format string, a ...interface{}) {
	logerror(errors.Errorf(format, a...))
}

func print(a ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	fmt.Fprintln(os.Stdout, file, ":", line, " ", fmt.Sprint(a...))
}

/*
	new algo:
		- pull down every record
		- check if 1/ flush time has elapsed 2/ the record's primary keys have already been seen
		- if true, flush the current buffer, and add the new record to the buffer (buffer max size = 500)
		- refresh pkeys every few minutes?
*/
func putRecordWorker(chanIdx int, stream *string, records <-chan *putRecordEntry, errorChan chan<- error) {
	for !done.IsSet() {
		kinesisClient := kinesis.New(session.New(aws.NewConfig()))
		buffer := make([]*kinesis.PutRecordsRequestEntry, 0)
		seenUniqRecords := make(map[string]bool)
		lastFlush := time.Now()
		bufSize := 0
		var lastCommitMsgInBuffer *pgx.WalMessage
		var pre *putRecordEntry
		var key string

		for !done.IsSet() {
			alreadySeen := false
			pre = nil

			select {
			case pre = <-records:
				if pre.pr.Operation == "COMMIT" {
					lastCommitMsgInBuffer = pre.msg
				}

				if pre.skip {
					continue
				}

				key = pre.pr.Relation
				relationPkeys, ok := pks.Load(pre.pr.Relation)
				var columnNames []string

				// here we construct a key out of the record
				// composed of its relation, and all of it's primary key values (if any)
				// if we do not have any primary keys, just use all the keys
				if ok {
					columnNames = relationPkeys.([]string)
				} else {
					columnNames := make([]string, len(pre.pr.Columns))

					i := 0
					for k := range pre.pr.Columns {
						columnNames[i] = k
						i++
					}
				}

				for _, col := range columnNames {
					key += col
					v, ok := pre.pr.Columns[col]
					if ok {
						if v.Quoted {
							key += "q"
						} else {
							key += "n"
						}
						key += v.Value
					}
				}

				_, alreadySeen = seenUniqRecords[key]
				seenUniqRecords[key] = true
			case <-time.After(FlushInterval):
				if len(buffer) == 0 {
					lastFlush = time.Now()
				}
			}

			// the message size is the data blob length + the partition key size
			msgSize := 0

			if pre != nil {
				msgSize = len(pre.json) + len([]byte(pre.pr.Relation))
			}

			if len(buffer) > 0 &&
				(alreadySeen || time.Since(lastFlush) >= FlushInterval || len(buffer) > maxRecordsPerRequest-1 || bufSize+msgSize >= maxRequestSize) {
				b := &backoff.Backoff{
					Jitter: true,
				}

				putSuccess := false

				for b.Attempt() < 100 && !done.IsSet() && !putSuccess {
					retryDuration := b.Duration()

					startTime := time.Now()
					out, err := kinesisClient.PutRecords(&kinesis.PutRecordsInput{
						StreamName: stream,
						Records:    buffer,
					})
					elapsed := time.Since(startTime)

					if err != nil {
						logerror(errors.Wrapf(err, "kinesis PutRecords failed; retrying failed records in %s", retryDuration.String()))
						kinesisClient = kinesis.New(session.New(aws.NewConfig())) // refresh the client to get new credentials etc.
						time.Sleep(retryDuration)
						continue
					} else if *out.FailedRecordCount > 0 {
						logerrf("%d records failed during Kinesis PutRecords; retrying in %s", *out.FailedRecordCount, retryDuration.String())
						originalRecordsCount := uint64(len(buffer))
						atomic.AddUint64(&stats.putRecordsTime, uint64(elapsed))
						buffer = failures(buffer, out.Records)
						atomic.AddUint64(&stats.putRecords, originalRecordsCount-uint64(len(buffer))) // total - unsent = sent
						time.Sleep(retryDuration)
						continue
					}

					putSuccess = true
					atomic.AddUint64(&stats.putRecordsTime, uint64(elapsed))
					atomic.AddUint64(&stats.putRecords, uint64(len(buffer)))
					if lastCommitMsgInBuffer != nil {
						ack(lastCommitMsgInBuffer, chanIdx)
					}
					buffer = nil
					seenUniqRecords = make(map[string]bool)
					lastFlush = time.Now()
					bufSize = 0
					break
				}

				if !putSuccess {
					errorChan <- errors.New("unable to replicate message after many attempts, or shutting down")
					return
				}
			}

			if pre != nil {
				buffer = append(buffer, &kinesis.PutRecordsRequestEntry{
					Data:         pre.json,
					PartitionKey: &pre.pr.Relation,
				})
				bufSize += msgSize
			}
		}
	}
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
	quoted := "false"
	if cv.Quoted {
		quoted = "true"
	}
	return map[string]string{"v": cv.Value, "t": cv.Type, "q": quoted}
}

func marshalColumnValuePair(newValue *parselogical.ColumnValue, oldValue *parselogical.ColumnValue) map[string]map[string]string {
	if oldValue != nil && newValue != nil {
		return map[string]map[string]string{
			"old": marshalColumnValue(oldValue),
			"new": marshalColumnValue(newValue),
		}
	} else if newValue != nil {
		return map[string]map[string]string{
			"new": marshalColumnValue(newValue),
		}
	} else if oldValue != nil {
		return map[string]map[string]string{
			"old": marshalColumnValue(oldValue),
		}
	}

	return nil
}

func marshalWALToJSON(pr *parselogical.ParseResult, msg *pgx.ReplicationMessage) ([]byte, error) {
	lsn := pgx.FormatLSN(msg.WalMessage.WalStart)
	time := time.Unix(0, int64(msg.WalMessage.ServerTime)).Format(time.RFC3339)
	columns := make(map[string]map[string]map[string]string)

	for k, v := range pr.Columns {
		oldV, ok := pr.OldColumns[k]

		if pr.Operation == "DELETE" {
			columns[k] = marshalColumnValuePair(nil, &v)
		} else {
			if ok && v.Value != oldV.Value {
				columns[k] = marshalColumnValuePair(&v, &oldV)
			} else {
				columns[k] = marshalColumnValuePair(&v, nil)
			}
		}
	}

	return json.Marshal(struct {
		Time      *string                                  `json:"time"`
		Lsn       *string                                  `json:"lsn"`
		Table     *string                                  `json:"table"`
		Operation *string                                  `json:"operation"`
		Columns   *map[string]map[string]map[string]string `json:"columns"`
	}{
		Time:      &time,
		Lsn:       &lsn,
		Table:     &pr.Relation,
		Operation: &pr.Operation,
		Columns:   &columns,
	})
}

func enqueueMsgForStream(r *putRecordEntry) error {
	if len(r.json) > maxRecordSize {
		return errors.New("replication messages must be less than 1MB in size")
	}

	if r.pr.Operation == "COMMIT" {
		atomic.StoreUint64(&maxRecvWal, r.msg.WalStart)

		for _, c := range messagesToStream {
			c <- r
		}
	} else if !r.skip {
		chanIdx, ok := tableToInternalChan[r.pr.Relation]

		if !ok {
			chanIdx = curTableChanIdx % maxSenders
			tableToInternalChan[r.pr.Relation] = chanIdx
			curTableChanIdx++
		}

		c, _ := messagesToStream[chanIdx]

		c <- r
	}

	return nil
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
		return enqueueMsgForStream(&putRecordEntry{pr: pr, msg: msg.WalMessage, skip: true, json: nil, stream: nil})
	}

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
		atomic.AddUint64(&stats.skipped, 1)
		return enqueueMsgForStream(&putRecordEntry{pr: pr, msg: msg.WalMessage, skip: true, json: nil, stream: nil})
	}

	switch pr.Operation {
	case "UPDATE":
		atomic.AddUint64(&stats.updates, 1)
	case "INSERT":
		atomic.AddUint64(&stats.inserts, 1)
	case "DELETE":
		atomic.AddUint64(&stats.deletes, 1)
	}

	err = pr.ParseColumns()

	if err != nil {
		return errors.Wrapf(err, "unable to parse columns of the replication message: %s", walString)
	}

	jsonRecord, err := marshalWALToJSON(pr, msg)

	if err != nil {
		return errors.Wrap(err, "error serializing WAL record into JSON")
	}

	return enqueueMsgForStream(&putRecordEntry{pr: pr, msg: msg.WalMessage, skip: false, json: jsonRecord, stream: stream})
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
		case msg = <-replicationMessages:
			err := handleReplicationMsg(msg, stream)

			if err != nil {
				replicationFinished <- err // already wrapped
				return
			}
		}
	}
}

func ack(msg *pgx.WalMessage, chanIdx int) {
	curMaxWal := atomic.LoadUint64(&maxAckWalPerChan[chanIdx])

	if msg.WalStart > curMaxWal {
		atomic.StoreUint64(&maxAckWalPerChan[chanIdx], msg.WalStart)
	}
}

func sendKeepalive(conn *pgx.ReplicationConn, force bool) error {
	if force || time.Since(lastStatus) >= DefaultKeepaliveTimeout {
		// iterate through the max actually persisted messages per channel and
		// mark the current max wal as min(max(fully replicated channels), not fully replicated channels)
		curMinUnackedWal := uint64(math.MaxUint64)
		curMaxAckedWal := uint64(0)
		recvd := atomic.LoadUint64(&maxRecvWal)
		for i := range maxAckWalPerChan {
			acked := atomic.LoadUint64(&maxAckWalPerChan[i])

			if acked == uint64(0) {
				continue
			}

			if acked > curMaxAckedWal {
				curMaxAckedWal = acked
			}

			if acked != recvd && acked < curMinUnackedWal {
				curMinUnackedWal = acked
			}
		}

		curWalMax := uint64(0)

		if curMinUnackedWal < curMaxAckedWal {
			curWalMax = curMinUnackedWal
		} else if curMaxAckedWal > 0 {
			curWalMax = curMaxAckedWal
		}

		status, err := pgx.NewStandbyStatus(curWalMax)
		if err != nil {
			return err
		}

		err = conn.SendStandbyStatus(status)
		if err != nil {
			return err
		}

		lastStatus = time.Now()
		atomic.StoreUint64(&maxWalSent, curWalMax)
	}

	return nil
}

func statsLoop() {
	for !done.IsSet() {
		time.Sleep(StatsInterval)

		timePerInsert := float64(0)
		putRecordsTime := atomic.LoadUint64(&stats.putRecordsTime)
		putRecords := atomic.LoadUint64(&stats.putRecords)
		inserts := atomic.LoadUint64(&stats.inserts)
		updates := atomic.LoadUint64(&stats.updates)
		deletes := atomic.LoadUint64(&stats.deletes)
		skipped := atomic.LoadUint64(&stats.skipped)
		lag := atomic.LoadUint64(&stats.lag)
		mws := atomic.LoadUint64(&maxWalSent)
		backlog := 0
		if time.Duration(putRecordsTime) > 0 {
			timePerInsert = (float64(putRecordsTime) / float64(time.Millisecond)) / float64(putRecords)
		}
		for i := 0; i < maxSenders; i++ {
			backlog += len(messagesToStream[i])
		}
		logf("inserts=%d (%.1f/s) updates=%d (%.1f/s) deletes=%d (%.1f/s) skipped=%d (%.1f/s) putrecords=%d (%.1f/s, %.0fms/record, %.1fs total) backlog=%d lsn=%s lag=%s",
			inserts, float64(inserts)/StatsInterval.Seconds(),
			updates, float64(updates)/StatsInterval.Seconds(),
			deletes, float64(deletes)/StatsInterval.Seconds(),
			skipped, float64(skipped)/StatsInterval.Seconds(),
			putRecords, float64(putRecords)/StatsInterval.Seconds(), timePerInsert, float64(putRecordsTime)/float64(time.Second),
			backlog,
			pgx.FormatLSN(mws),
			humanize.Bytes(lag))
		atomic.StoreUint64(&stats.inserts, 0)
		atomic.StoreUint64(&stats.updates, 0)
		atomic.StoreUint64(&stats.deletes, 0)
		atomic.StoreUint64(&stats.skipped, 0)
		atomic.StoreUint64(&stats.putRecords, 0)
		atomic.StoreUint64(&stats.putRecordsTime, 0)
	}
}

func fetchPKs(conn *pgx.Conn) error {
	rows, err := conn.Query(`
	  SELECT
      tc.table_schema || '.' || tc.table_name AS table_schema,
      array_agg(kcu.column_name::text) AS pkeys
    FROM
      information_schema.table_constraints tc
    LEFT JOIN
      information_schema.key_column_usage kcu
      ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name
    WHERE
      tc.constraint_type = 'PRIMARY KEY'
    GROUP BY
      tc.table_schema || '.' || tc.table_name`)
	if err != nil {
		return err
	}

	defer rows.Close()

	// Iterate through the result set
	for rows.Next() {
		var schemaTable string
		var pk []string

		err = rows.Scan(&schemaTable, &pk)
		if err != nil {
			return err
		}

		pks.Store(schemaTable, pk)
	}

	if rows.Err() != nil {
		return err
	}

	return nil
}

func fetchLag(slot *string, conn *pgx.Conn) error {
	var lag uint64

	err := conn.QueryRow(fmt.Sprintf(`
	 	SELECT
	  	pg_current_xlog_location() - restart_lsn
	 	FROM pg_replication_slots
	 	WHERE slot_name = '%s';`, *slot)).Scan(&lag)

	if err != nil {
		return err
	}

	atomic.StoreUint64(&stats.lag, lag)
	return nil
}

func pkeyLagLoop(slot *string, nonReplConn *pgx.Conn, replicationFinished chan error) {
	for !done.IsSet() {
		err := fetchPKs(nonReplConn)
		if err != nil {
			replicationFinished <- err
			return
		}
		err = fetchLag(slot, nonReplConn)
		if err != nil {
			replicationFinished <- err
			return
		}
		time.Sleep(StatsInterval - 1)
	}
}

func connectReplicateLoop(slot *string, sourceConfig pgx.ConnConfig, stream *string) error {
	var err error
	var replErr error

	// have to make a copy, as pgx stores the dialer information in the ConnConfig
	// which is not compatible across replication and non-replication connections
	// todo - less ugly way of accomplishing this...
	sourceConfigCpy := pgx.ConnConfig{Host: sourceConfig.Host, Port: sourceConfig.Port, Database: sourceConfig.Database, User: sourceConfig.User, Password: sourceConfig.Password, TLSConfig: sourceConfig.TLSConfig, UseFallbackTLS: sourceConfig.UseFallbackTLS, FallbackTLSConfig: sourceConfig.FallbackTLSConfig}

	nonReplConn, err := pgx.Connect(sourceConfigCpy)
	if err != nil {
		return errors.Wrapf(err, "unable to establish connection to target DB %s/%s", sourceConfig.Host, sourceConfig.Database)
	}
	defer nonReplConn.Close()

	conn, err := pgx.ReplicationConnect(sourceConfig)
	if err != nil {
		return errors.Wrapf(err, "unable to establish replication connection to source DB %s/%s", sourceConfig.Host, sourceConfig.Database)
	}
	defer conn.Close()

	err = conn.StartReplication(*slot, 0, -1)
	if err != nil {
		return errors.Wrapf(err, "unable to start replication to slot %s", *slot)
	}

	lastStatus = time.Now()
	initiallyConnected = true

	replicationMessages := make(chan *pgx.ReplicationMessage)
	replicationFinished := make(chan error, 1)
	messagesToStream = make(map[int]chan *putRecordEntry)
	tableToInternalChan = make(map[string]int)
	maxAckWalPerChan = make([]uint64, maxSenders)
	curTableChanIdx = 0

	go pkeyLagLoop(slot, nonReplConn, replicationFinished)
	go replicationLoop(replicationMessages, replicationFinished, stream)

	for i := 0; i < maxSenders; i++ {
		messagesToStream[i] = make(chan *putRecordEntry, maxBacklog)
		go putRecordWorker(i, stream, messagesToStream[i], replicationFinished)
	}

	logf("replication starting from LSN %s", pgx.FormatLSN(maxWalSent))

	go statsLoop()

	for !done.IsSet() {
		var message *pgx.ReplicationMessage
		keepaliveRequested := false

		replicationCtx, cancelFn := context.WithTimeout(context.Background(), ReplicationLoopInterval)
		message, err = conn.WaitForReplicationMessage(replicationCtx)
		cancelFn()
		now := time.Now()

		if err != nil && err != context.DeadlineExceeded {
			return errors.Wrap(err, "waiting for replication message failed")
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
				// this is not exactly the server time
				// but we are taking over this field as PG does not send it down
				message.WalMessage.ServerTime = uint64(now.UnixNano())
				replicationMessages <- message
			} else if message.ServerHeartbeat != nil {
				keepaliveRequested = message.ServerHeartbeat.ReplyRequested == 1
			}
		}

		err = sendKeepalive(conn, keepaliveRequested)
		if err != nil {
			return errors.Wrap(err, "unable to send keepalive")
		}
	}

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
	flag.Var(&tables, "table", "")
	flag.Var(&tables, "t", "")
	flag.Var(&excludedTables, "exclude-table", "")
	flag.Var(&excludedTables, "T", "")

	showVersion := flag.Bool("version", false, "")

	flag.Parse()

	if *showVersion {
		fmt.Printf("pg_kinesis %s\n", version)
		os.Exit(0)
	}

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
		err := createReplicationSlot(slot, sourceConfig)
		logerror(err)

		if err != nil {
			if strings.HasSuffix(err.Error(), "(SQLSTATE 42710)") {
				logf("replication slot %s already exists, continuing", *slot)
			} else {
				os.Exit(1)
			}
		}
	}

	if *drop {
		err := dropReplicationSlot(slot, sourceConfig)
		logerror(err)

		if err != nil {
			os.Exit(1)
		}
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
		} else {
			if err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
}
