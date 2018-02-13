package main

type defaultJSON struct {
	Time      *string                                  `json:"time"`
	Lsn       *string                                  `json:"lsn"`
	Table     *string                                  `json:"table"`
	Operation *string                                  `json:"operation"`
	Columns   *map[string]map[string]map[string]string `json:"columns"`
}
