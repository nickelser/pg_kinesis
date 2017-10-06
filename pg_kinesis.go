package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

import (
	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/jackc/pgx"
	"github.com/nickelser/parselogical"

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
  --retry-slot          If this flag is present, retry the initial connection to the replication slot; useful for high-availability setups where the same pg_kinesis command is run from multiple hosts.
`

// DefaultKeepaliveTimeout is the time before we proactively send a keepalive & status update
const DefaultKeepaliveTimeout = 5 * time.Second

// ReplicationLoopInterval is the time between update checks
const ReplicationLoopInterval = 1 * time.Second

// ReconnectInterval is the time between connection attempts
const ReconnectInterval = 1 * time.Second

// StatsInterval is the time between statistics reporting
const StatsInterval = 5 * time.Second

var walLock sync.Mutex
var maxWal uint64
var maxWalSent uint64
var lastStatus time.Time

var stats struct {
	sync.Mutex

	updates uint64
	inserts uint64
	deletes uint64
	skipped uint64
}

type tableList map[string]bool

var tables tableList
var hasTables bool

var stmts map[string]bool
var pks map[string]string
var cols map[string]map[string]bool

var sigs = make(chan os.Signal, 1)
var restart = make(chan bool, 1)
var shutdown = make(chan bool, 1)
var done = abool.New()

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

func stmtNameFromPTD(ptd *parselogical.ParsedTestDecoding) (string, []string) {
	h := fnv.New64a()

	h.Write([]byte(ptd.Schema))
	h.Write([]byte(ptd.Table))

	keys := make([]string, 0, len(ptd.Fields))
	for k := range ptd.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys) // ensure the fields are sorted

	for _, k := range keys {
		h.Write([]byte(k))
	}

	return fmt.Sprintf("insert_%x", h.Sum64()), keys
}

func prepareStmtFromParsed(conn *pgx.Conn, ptd *parselogical.ParsedTestDecoding) (string, []string, error) {
	stmtName, keys := stmtNameFromPTD(ptd)

	if _, ok := stmts[stmtName]; ok {
		return stmtName, keys, nil
	}

	pk, ok := pks[ptd.SchemaTable]

	if !ok {
		return stmtName, keys, errors.Errorf("unknown primary key for table %s", ptd.SchemaTable)
	}

	// TODO: decide on variable column handling
	// cmap, ok := cols[tableSchema]

	// if !ok {
	// 	return stmtName, keys, errors.Errorf("unknown columns list for table %s", tableSchema)
	// }

	insertPrefix := fmt.Sprintf("INSERT INTO %s (", ptd.SchemaTable)
	values := "VALUES ("
	conflictSuffix := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET ", pk)

	for i, k := range keys {
		insertPrefix += k + ","
		values += fmt.Sprintf("$%d,", i+1)
		if k != pk {
			conflictSuffix += fmt.Sprintf("%s = excluded.%s,", k, k)
		}
	}

	conflictSuffix = strings.TrimSuffix(conflictSuffix, ",")
	insertPrefix = strings.TrimSuffix(insertPrefix, ",") + ") "
	values = strings.TrimSuffix(values, ",") + ") "

	_, err := conn.PrepareEx(stmtName, insertPrefix+values+conflictSuffix, nil)

	if err != nil {
		return "", keys, err
	}

	stmts[stmtName] = true

	return stmtName, keys, nil
}

func replicateMsg(targetConn *pgx.Conn, msg *pgx.ReplicationMessage) error {
	var err error

	walString := string(msg.WalMessage.WalData)
	ptd := parselogical.NewParsedTestDecoding(walString)
	err = ptd.ParsePrelude()

	if err != nil {
		return errors.Wrapf(err, "unable to parse table or operation type of replication message: %s", walString)
	}

	if ptd.Transaction != "" {
		ack(msg)
		return nil
	}

	stats.Lock()
	defer stats.Unlock()

	if hasTables {
		if _, ok := tables[ptd.SchemaTable]; !ok {
			stats.skipped++
			ack(msg)
			return nil
		}
	}

	switch ptd.Operation {
	case "UPDATE":
		stats.updates++
	case "INSERT":
		stats.inserts++
	case "DELETE":
		stats.deletes++
	}

	if ptd.Operation != "UPDATE" && ptd.Operation != "INSERT" {
		ack(msg)
		return nil
	}

	err = ptd.ParseColumns()

	if err != nil {
		return errors.Wrapf(err, "unable to parse columns of the replication message: %s", walString)
	}

	preparedStmt, keys, err := prepareStmtFromParsed(targetConn, ptd)

	if err != nil {
		return errors.Wrap(err, "unable to prepare insertion statement")
	}

	toInsert := make([]interface{}, len(ptd.Fields))

	for i, k := range keys {
		v := ptd.Fields[k]

		if v.Valid {
			toInsert[i] = v.String
		} else {
			toInsert[i] = nil
		}
	}

	_, err = targetConn.Exec(preparedStmt, toInsert...)

	if err != nil {
		return errors.Wrap(err, "unable to insert row")
	}

	ack(msg)

	return nil
}

func replicationLoop(targetConn *pgx.Conn, replicationMessages chan *pgx.ReplicationMessage, replicationFinished chan error) {
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
		}

		err := replicateMsg(targetConn, msg)

		if err != nil {
			replicationFinished <- err // already wrapped
			return
		}
	}
}

func ack(msg *pgx.ReplicationMessage) {
	walLock.Lock()
	defer walLock.Unlock()

	if msg.WalMessage.WalStart > maxWal {
		maxWal = msg.WalMessage.WalStart
	}
}

func sendKeepalive(conn *pgx.ReplicationConn, force bool) error {
	walLock.Lock()
	defer walLock.Unlock()

	if force || time.Since(lastStatus) >= DefaultKeepaliveTimeout || maxWal > maxWalSent {
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
	}

	return nil
}

func fetchPKs(conn *pgx.Conn) error {
	rows, err := conn.Query(`
		SELECT tc.table_schema || '.' || tc.table_name, kc.column_name
		FROM
			information_schema.table_constraints tc,
			information_schema.key_column_usage kc
		WHERE
			tc.constraint_type = 'PRIMARY KEY'
			AND kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
			AND kc.constraint_name = tc.constraint_name`)
	if err != nil {
		return err
	}

	defer rows.Close()

	pks = make(map[string]string)

	// Iterate through the result set
	for rows.Next() {
		var schemaTable string
		var pk string

		err = rows.Scan(&schemaTable, &pk)
		if err != nil {
			return err
		}

		pks[schemaTable] = pk
	}

	if rows.Err() != nil {
		return err
	}

	return nil
}

func fetchColumns(conn *pgx.Conn) error {
	rows, err := conn.Query(`
		SELECT table_schema || '.' || table_name, ARRAY_AGG(column_name::text) AS columns
		FROM information_schema.columns
		WHERE table_schema != 'pg_catalog'
					AND table_schema != 'information_schema'
		GROUP BY table_schema, table_name`)
	if err != nil {
		return err
	}

	defer rows.Close()

	cols = make(map[string]map[string]bool)

	// Iterate through the result set
	for rows.Next() {
		var tableSchema string
		var tableCols []string

		cmap := make(map[string]bool)

		err = rows.Scan(&tableSchema, &tableCols)
		if err != nil {
			return err
		}

		for _, col := range tableCols {
			cmap[col] = true
		}

		cols[tableSchema] = cmap
	}

	if rows.Err() != nil {
		return err
	}

	return nil
}

func connectReplicateLoop(slot *string, sourceConfig pgx.ConnConfig, targetConfig pgx.ConnConfig) error {
	var err error
	var replErr error

	conn, err := pgx.ReplicationConnect(sourceConfig)
	if err != nil {
		return errors.Wrapf(err, "unable to establish connection to source DB %s/%s", sourceConfig.Host, sourceConfig.Database)
	}
	defer conn.Close()

	targetConn, err := pgx.Connect(targetConfig)
	if err != nil {
		return errors.Wrapf(err, "unable to establish connection to target DB %s/%s", targetConfig.Host, targetConfig.Database)
	}
	defer targetConn.Close()

	err = fetchPKs(targetConn)
	if err != nil {
		return errors.Wrap(err, "unable to fetch primary keys from target DB")
	}

	err = fetchColumns(targetConn)
	if err != nil {
		return errors.Wrap(err, "unable to fetch column listing from target DB")
	}

	if hasTables {
		for tbl := range tables {
			if _, ok := cols[tbl]; !ok {
				logerrf("missing table '%s' on target DB", tbl)
				os.Exit(1)
			}
		}
	}

	err = conn.StartReplication(*slot, 0, -1)
	if err != nil {
		return errors.Wrapf(err, "unable to start replication to slot %s", *slot)
	}

	lastStatus = time.Now()

	replicationMessages := make(chan *pgx.ReplicationMessage)
	replicationFinished := make(chan error, 1)
	stmts = make(map[string]bool)

	lastStats := time.Now()

	go replicationLoop(targetConn, replicationMessages, replicationFinished)

	logf("replication started at %v starting from LSN %s", lastStatus, pgx.FormatLSN(maxWalSent))

	for !done.IsSet() {
		var message *pgx.ReplicationMessage
		keepaliveRequested := false

		message, err = conn.WaitForReplicationMessage(ReplicationLoopInterval)

		logf("got message! %s", message)

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

		sinceLastStats := time.Since(lastStats)
		if sinceLastStats >= StatsInterval {
			stats.Lock()
			logf("stats: inserts %d (%.1f/s), updates %d (%.1f/s), deletes %d (%.1f/s), skipped %d (%.1f/s), confirmed LSN %s",
				stats.inserts, float64(stats.inserts)/sinceLastStats.Seconds(),
				stats.updates, float64(stats.updates)/sinceLastStats.Seconds(),
				stats.deletes, float64(stats.deletes)/sinceLastStats.Seconds(),
				stats.skipped, float64(stats.skipped)/sinceLastStats.Seconds(),
				pgx.FormatLSN(maxWalSent))
			stats.inserts = 0
			stats.updates = 0
			stats.deletes = 0
			stats.skipped = 0
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

func (tl *tableList) Set(value string) error {
	if len(*tl) > 0 {
		return errors.New("tables flag already set")
	}
	for _, t := range strings.Split(value, ",") {
		if !strings.Contains(t, ".") {
			return fmt.Errorf("tables argument '%s' does not specify schema", t)
		}
		(*tl)[strings.TrimSpace(t)] = true
		hasTables = true
	}
	return nil
}

func (tl *tableList) String() string {
	return fmt.Sprint(*tl)
}

func main() {
	tables = make(tableList)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usage)
	}

	sourceURI := flag.String("source", "", "")
	targetURI := flag.String("target", "", "")
	create := flag.Bool("create", false, "")
	drop := flag.Bool("drop", false, "")
	slot := flag.String("slot", "", "")
	flag.Var(&tables, "tables", "")

	flag.Parse()

	if (*sourceURI == "" || *targetURI == "" || *slot == "") ||
		(*create && *drop) {
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	sourceConfig, err := pgx.ParseConnectionString(*sourceURI)
	if err != nil {
		logerror(errors.Wrapf(err, "unable to parse source DB URI '%s'", *sourceURI))
		os.Exit(1)
	}

	targetConfig, err := pgx.ParseConnectionString(*targetURI)
	if err != nil {
		logerror(errors.Wrapf(err, "unable to parse target DB URI '%s'", *targetURI))
		os.Exit(1)
	}

	if *create {
		logerror(createReplicationSlot(slot, sourceConfig))
		os.Exit(0)
	}

	if *drop {
		logerror(dropReplicationSlot(slot, sourceConfig))
		os.Exit(0)
	}

	go func() {
		sig := <-sigs
		logerrf("received signal: %s, shutting down", sig)

		// non-blocking send to shutdown
		select {
		case shutdown <- true:
		default:
		}

		done.SetTo(true)
	}()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for !done.IsSet() {
		err := connectReplicateLoop(slot, sourceConfig, targetConfig)
		logerror(err)

		if !done.IsSet() {
			// non-blocking send to restart
			select {
			case restart <- true:
			default:
			}

			time.Sleep(ReconnectInterval)
		}
	}
}
