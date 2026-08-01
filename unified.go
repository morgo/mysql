// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
)

// QueryResultContext executes query and returns the server's response in the
// form the MySQL protocol describes it: exactly one of rows or result is
// non-nil on success. A statement that produces a resultset (even an empty
// one) yields rows; a statement answered with an OK packet (INSERT, UPDATE,
// DDL, SET, ...) yields result, carrying the affected-row count and last
// insert id.
//
// database/sql callers must choose between QueryContext and ExecContext
// before executing. A caller that receives arbitrary SQL (a proxy, a REPL)
// therefore has to classify statements up front, and a misclassification
// either discards a resultset (Exec) or loses the OK metadata (Query). This
// method lets such callers branch on what the server actually sent instead.
// It is intended to be reached through (*sql.Conn).Raw and a structural
// interface assertion:
//
//	err := conn.Raw(func(dc any) error {
//		uq := dc.(interface {
//			QueryResultContext(context.Context, string, []driver.NamedValue) (driver.Rows, driver.Result, error)
//		})
//		rows, result, err := uq.QueryResultContext(ctx, query, args)
//		...
//	})
//
// Because Raw bypasses database/sql's argument conversion, args are
// normalized here with CheckNamedValue, exactly as database/sql would do
// before QueryContext. As with QueryContext, non-empty args require
// InterpolateParams; otherwise driver.ErrSkip is returned before anything is
// written. driver.ErrBadConn is returned only when no command reached the
// server, so the caller may safely retry on it. When rows is non-nil the
// connection is busy until rows.Close.
//
// Unlike QueryContext, rows delivers every non-NULL cell as its MySQL wire
// text: a []byte aliasing the connection's read buffer, valid only until the
// next Next or Close call. Nothing is parsed into Go types (parseTime does
// not apply), so values forward verbatim — zero dates, float representations,
// and fractional-second padding survive untouched.
func (mc *mysqlConn) QueryResultContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, driver.Result, error) {
	if mc.closed.Load() {
		return nil, nil, driver.ErrBadConn
	}

	dargs := make([]driver.Value, len(args))
	for i := range args {
		nv := args[i]
		if err := mc.CheckNamedValue(&nv); err != nil {
			return nil, nil, err
		}
		dargs[i] = nv.Value
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, nil, err
	}

	rows, result, err := mc.queryResult(query, dargs)
	if err != nil || rows == nil {
		// Error, or a complete OK response: the connection is idle again.
		mc.finish()
		return nil, result, err
	}
	// Resultset: the context watcher stays armed until the caller finishes
	// reading, mirroring QueryContext.
	rows.finish = mc.finish
	return rows, nil, nil
}

// queryResult is the transport half of QueryResultContext. It mirrors
// mysqlConn.query, except that an OK response is surfaced as a driver.Result
// (the way Exec reports it) instead of being hidden inside an empty, done
// resultset.
func (mc *mysqlConn) queryResult(query string, args []driver.Value) (*textRows, driver.Result, error) {
	handleOk := mc.clearResult()

	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, nil, err
		}
		query = prepared
	}

	// Send command
	if err := mc.writeCommandPacketStr(comQuery, query); err != nil {
		return nil, nil, mc.markBadConn(err)
	}

	// Read Result
	resLen, _, err := handleOk.readResultSetHeaderPacket()
	if err != nil {
		return nil, nil, err
	}

	if resLen == 0 {
		// OK packet: no resultset follows. Drain any trailing results of a
		// multi-statement exactly like exec, then surface the accumulated
		// result the same way Exec does.
		if err := handleOk.discardResults(); err != nil {
			return nil, nil, err
		}
		copied := mc.result
		return nil, &copied, nil
	}

	// Columns
	rows := new(textRows)
	rows.mc = mc
	rows.raw = true
	rows.rs.columns, err = mc.readColumns(resLen, nil)
	if err != nil {
		return nil, nil, err
	}
	return rows, nil, nil
}
