package main

import (
	"github.com/jackc/pgx"
	"github.com/nickelser/parselogical"
)

type putRecordEntry struct {
	stream *string
	msg    *pgx.WalMessage
	pr     *parselogical.ParseResult
	skip   bool
	key    *string
	json   []byte
}
