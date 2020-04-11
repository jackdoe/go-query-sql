package inv

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"

	iq "github.com/rekki/go-query"
	"github.com/rekki/go-query/util/analyzer"
	spec "github.com/rekki/go-query/util/go_query_dsl"
	"github.com/rekki/go-query/util/index"
)

type LiteIndex struct {
	perField          map[string]*analyzer.Analyzer
	db                *sql.DB
	table             string
	TotalNumberOfDocs int
	concat            ConcatQuery
}

func NewLiteIndex(db *sql.DB, concat ConcatQuery, tableName string, perField map[string]*analyzer.Analyzer) (*LiteIndex, error) {
	if perField == nil {
		perField = map[string]*analyzer.Analyzer{}
	}

	d := &LiteIndex{TotalNumberOfDocs: 1, perField: perField, db: db, table: tableName, concat: concat}

	err := d.Recreate()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *LiteIndex) Index(docs ...index.DocumentWithID) error {
	var sb strings.Builder

	todo := map[string][]int32{}

	for _, doc := range docs {
		did := doc.DocumentID()

		fields := doc.IndexableFields()
		for field, value := range fields {
			if len(field) == 0 {
				continue
			}

			analyzer, ok := d.perField[field]
			if !ok {
				analyzer = index.DefaultAnalyzer
			}
			for _, v := range value {
				tokens := analyzer.AnalyzeIndex(v)
				for _, t := range tokens {
					if len(t) == 0 {
						continue
					}

					sb.WriteString(field)
					sb.WriteRune('/')
					sb.WriteString(t)
					s := sb.String()
					todo[s] = append(todo[s], did)
					sb.Reset()
				}
			}
		}
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	for t, docs := range todo {
		err := d.add(tx, t, docs)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (d *LiteIndex) Parse(input *spec.Query) (iq.Query, error) {
	return index.Parse(input, func(k, v string) iq.Query {
		terms := d.Terms(k, v)
		if len(terms) == 1 {
			return terms[0]
		}
		return iq.Or(terms...)
	})
}

type ConcatQuery string

const (
	SQLITE3 ConcatQuery = "SET list = list || $1 WHERE id=$2"
	MYSQL   ConcatQuery = "SET list = concat(list,$1) WHERE id=$2"
)

func (d *LiteIndex) add(tx *sql.Tx, t string, docs []int32) error {
	b := make([]byte, 4*len(docs))
	for i, did := range docs {
		binary.LittleEndian.PutUint32(b[i*4:], uint32(did))
	}
	var exists string
	err := tx.QueryRow("SELECT id FROM "+d.table+" where id = $1", t).Scan(&exists)
	if err == nil {
		_, err := tx.Exec("UPDATE "+d.table+" "+string(d.concat), b, t)
		if err != nil {
			return err
		}
	} else {
		_, err := tx.Exec("INSERT INTO "+d.table+"(id,list) VALUES($1,$2)", t, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *LiteIndex) Terms(field string, term string) []iq.Query {
	analyzer, ok := d.perField[field]
	if !ok {
		analyzer = index.DefaultAnalyzer
	}
	tokens := analyzer.AnalyzeSearch(term)
	queries := []iq.Query{}
	for _, t := range tokens {
		queries = append(queries, d.NewTermQuery(field, t))
	}
	return queries
}

func (d *LiteIndex) NewTermQuery(field string, term string) iq.Query {
	if len(field) == 0 || len(term) == 0 {
		return iq.Term(d.TotalNumberOfDocs, fmt.Sprintf("broken(%s:%s)", field, term), []int32{})
	}

	t := field + "/" + term
	list := []byte{}

	err := d.db.QueryRow("select list from "+d.table+" where id=?", t).Scan(&list)
	if err == nil {
		postings := make([]int32, len(list)/4)
		for i := 0; i < len(postings); i++ {
			from := i * 4
			postings[i] = int32(binary.LittleEndian.Uint32(list[from : from+4]))
		}
		return iq.Term(d.TotalNumberOfDocs, t, postings)
	} else {
		return iq.Term(d.TotalNumberOfDocs, fmt.Sprintf("missing(%s:%s)", field, term), []int32{})
	}
}

func (d *LiteIndex) Close() {
	d.db.Close()
}

func (d *LiteIndex) Foreach(query iq.Query, cb func(int32, float32)) {
	for query.Next() != iq.NO_MORE {
		did := query.GetDocId()
		score := query.Score()

		cb(did, score)
	}
}

func (d *LiteIndex) Truncate() error {
	_, err := d.db.Exec("drop table " + d.table)
	if err != nil {
		return err
	}

	return d.Recreate()
}

func (d *LiteIndex) Recreate() error {
	_, err := d.db.Exec("CREATE TABLE IF NOT EXISTS " + d.table + " (id varchar(255) primary key, list largeblob)")
	return err
}
