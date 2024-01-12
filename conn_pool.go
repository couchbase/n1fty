package n1fty

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/query/logging"
	gometrics "github.com/rcrowley/go-metrics"
	"google.golang.org/grpc"
)

const (
	// This is in sync with ewma that we are using (created with NewEWMA5())
	connReleaseInterval = 5 // Seconds

	// This is to log active and free connection count history.
	connCountLogInterval = 60 // Seconds
)

var (
	errClosedPool  = errors.New("conn pool is Closed")
	errNoPool      = errors.New("conn pool is not initialized (is nil)")
	errPoolTimeout = errors.New("conn pool timeout")
)

// Default Pool Config
var (
	poolSize         = 5000
	poolOverflow     = 30
	minPoolSize      = int32(1000)
	relConnBatchSize = int32(100)
	getTimeout       = time.Duration(1000)
	availTimeout     = time.Duration(1)
)

// To avoid overhead of creating a new conn object for each query, we
// maintain a pool of conn objects for each node.
// The pool is a channel of conn objects with a max size of "poolSize".
// It keeps track of "activeConns" and "freeConns".
//
// When a query comes in,
// it first try to get a free conn from the pool and return it.
// otherwise, it will wait for "availTimeout", for a conn to become available.
// Last resort is to create a new conn, for which it consult "createsem" to see
// if it can create a new conn. All this is done within "getTimeout" duration.
//
// It has an overflow mechanism to ensure, even if the pool is full, we can
// still create new connections. This is to handle QPS spikes.
// But, we don't want to create too many connections, so we limit the
// quota of conn creation to be "poolSize+poolOverflow".
//
// After query is done, it return the conn back to the pool.
// If pool is full, it close the conn, which make room for a new
// conn to be created.
//
// It also has releaseConnsRoutine(), a smart routine to make sure we don't
// have too many free conns sitting in the pool.
// It samples #activeConns every second, and calculates ewma every 5 seconds
// (based on the last 5 samples and previous ewma)
// Before closing freeConns (in batch of relConnBatchSize), it retains
// an amount of max(activeConns, ewma, minPoolSizeWM) conns in the pool.
// Which means, In case of a QPS spike (activeConns > ewma), it will retain
// #activeConns conns in the pool. And, In case of a QPS regression
// (activeConns < ewma), it will retain #ewma conns in the pool, which is done
// to avoid creating new conns once we come out of a QPS regression.
type connPool struct {
	host        string
	connections chan *grpc.ClientConn
	createsem   chan bool // Semaphore, limits #totalConnections
	stopCh      chan bool

	// stats

	activeConns int32 // in-use connections
	freeConns   int32 // idle connections sitting in pool
	ewma        gometrics.EWMA

	// config params

	// timeout for getting a connection from pool (or creating a new one)
	getTimeout time.Duration
	// wait time for a free connection to become available
	availTimeout time.Duration
	// min number of connections to be retained in pool once created
	minPoolSizeWM    int32 // Low Water Mark
	relConnBatchSize int32 // #conns to be closed per release cycle
	logPrefix        string
}

func newConnPool(host string) *connPool {
	cp := &connPool{
		host:        host,
		connections: make(chan *grpc.ClientConn, poolSize),
		createsem:   make(chan bool, poolSize+poolOverflow),
		stopCh:      make(chan bool, 1),

		getTimeout:       getTimeout,
		availTimeout:     availTimeout,
		minPoolSizeWM:    minPoolSize,
		relConnBatchSize: relConnBatchSize,
		logPrefix:        fmt.Sprintf("n1fty: connPool:%v", host),
	}

	cp.ewma = gometrics.NewEWMA5()
	logging.Infof("%v started conn pool with poolsize:%v, overflow:%v, "+
		"lowWM:%v, relConnBatchSize:%v", cp.logPrefix, poolSize,
		poolOverflow, minPoolSize, relConnBatchSize)

	go cp.releaseConnsRoutine()

	return cp
}

func (cp *connPool) mkConn(host string) (*grpc.ClientConn, error) {
	connOpts, ok := ftsClientInst.connOpts.get(host)
	if !ok {
		logging.Errorf("%v failed to make connection object, no grpc options "+
			"for host:%v", cp.logPrefix, host)
		return nil, fmt.Errorf("no grpc options for host:%v", host)
	}

	conn, err := grpc.Dial(host, connOpts...)
	if err != nil {
		logging.Errorf("n1fty: client: grpc.Dial for host: %s, err: %v",
			host, err)
		return nil, err
	}

	return conn, nil
}

func (cp *connPool) getWithTimeout(
	d time.Duration) (connectn *grpc.ClientConn, err error) {
	if cp == nil {
		return nil, errNoPool
	}

	// short-circuit available connetions.
	select {
	case connectn, ok := <-cp.connections:
		if !ok {
			return nil, errClosedPool
		}
		logging.Debugf("%v get connection from pool", cp.logPrefix)
		atomic.AddInt32(&cp.freeConns, -1)
		atomic.AddInt32(&cp.activeConns, 1)
		return connectn, nil
	default:
	}
	t := time.NewTimer(cp.availTimeout * time.Millisecond)
	defer t.Stop()

	// Try to grab an available connection within 1ms
	select {
	case connectn, ok := <-cp.connections:
		if !ok {
			return nil, errClosedPool
		}
		logging.Debugf("%v get connection (avail1) from pool", cp.logPrefix)
		atomic.AddInt32(&cp.freeConns, -1)
		atomic.AddInt32(&cp.activeConns, 1)
		return connectn, nil

	case <-t.C:
		// No connection came around in time, let's see
		// whether we can get one or build a new one first.
		t.Reset(d) // Reuse the timer for the full timeout.
		select {
		case connectn, ok := <-cp.connections:
			if !ok {
				return nil, errClosedPool
			}
			logging.Debugf("%v get connection (avail2) from pool",
				cp.logPrefix)
			atomic.AddInt32(&cp.freeConns, -1)
			atomic.AddInt32(&cp.activeConns, 1)
			return connectn, nil

		case cp.createsem <- true: // create a new connection
			// This can potentially be an overflow connection, or
			// a pooled connection.
			connectn, err := cp.mkConn(cp.host)
			if err != nil {
				// On error, release our create hold
				<-cp.createsem
			} else {
				atomic.AddInt32(&cp.activeConns, 1)
			}
			logging.Debugf("%v new connection (create) from pool", cp.logPrefix)
			return connectn, err

		case <-t.C:
			return nil, errPoolTimeout
		}
	}
}

func (cp *connPool) get() (*grpc.ClientConn, error) {
	return cp.getWithTimeout(cp.getTimeout * time.Millisecond)
}

// return back the connection to pool
func (cp *connPool) yield(connectn *grpc.ClientConn) {
	defer atomic.AddInt32(&cp.activeConns, -1)

	if connectn == nil {
		return
	}

	if cp == nil {
		logging.Infof("%v pool closed, closing the returned conn",
			cp.logPrefix)
		connectn.Close()
		return
	}

	select {
	case cp.connections <- connectn:
		logging.Debugf("%v connection reclaimed to pool", cp.logPrefix)
		atomic.AddInt32(&cp.freeConns, 1)
	default:
		logging.Debugf("%v closing overflow connection, poolSize=%v",
			cp.logPrefix, len(cp.connections))
		<-cp.createsem
		connectn.Close()
	}
}

func (cp *connPool) close(reason string) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v Close() request with reason:%v, crashed: %v",
				cp.logPrefix, reason, r)
		}
	}()
	cp.stopCh <- true
	close(cp.connections)
	close(cp.createsem)
	for conn := range cp.connections {
		conn.Close()
	}
	logging.Infof("%v pool closed, reason:%v", cp.logPrefix, reason)
	return
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// To handle QPS Spikes and Regressions (relative to Avg)
func (cp *connPool) numConnsToRetain() (int32, bool) {
	avg := cp.ewma.Rate()
	act := atomic.LoadInt32(&cp.activeConns)
	num := max(act, int32(avg))
	num = max(cp.minPoolSizeWM, num)
	fc := atomic.LoadInt32(&cp.freeConns)
	totalConns := act + fc
	if totalConns-cp.relConnBatchSize >= num {
		// Don't release more than relConnBatchSize number of connections
		// in 1 iteration
		logging.Debugf("%v releasinng connections", cp.logPrefix)
		return totalConns - cp.relConnBatchSize, true
	}
	return totalConns, false
}

func (cp *connPool) releaseConns(numRetConns int32) {
	for {
		fc := atomic.LoadInt32(&cp.freeConns)
		act := atomic.LoadInt32(&cp.activeConns)
		totalConns := act + fc
		if totalConns > numRetConns && fc > 0 {
			select {
			case conn, ok := <-cp.connections:
				if !ok {
					return
				}
				atomic.AddInt32(&cp.freeConns, -1)
				conn.Close()
			default:
				break
			}
		} else {
			break
		}
	}
}

func (cp *connPool) releaseConnsRoutine() {
	sampleT := time.NewTicker(time.Second)
	releaseT := time.NewTicker(connReleaseInterval * time.Second)
	logT := time.NewTicker(connCountLogInterval * time.Second)

	for {
		select {
		case <-cp.stopCh:
			logging.Infof("%v Stopping releaseConnsRoutine", cp.logPrefix)
			return
		case <-sampleT.C: // sample #activeConns for ewma calculation
			act := atomic.LoadInt32(&cp.activeConns)
			cp.ewma.Update(int64(act))
		case <-releaseT.C:
			// calculate ewma
			cp.ewma.Tick()

			// release conns if needed
			numRetConns, needToFreeConns := cp.numConnsToRetain()
			if needToFreeConns {
				cp.releaseConns(numRetConns)
			}
		case <-logT.C: // Log active and free conns count
			act := atomic.LoadInt32(&cp.activeConns)
			fc := atomic.LoadInt32(&cp.freeConns)
			logging.Infof("%v active conns %v, free conns %v",
				cp.logPrefix, act, fc)
		}
	}
}
