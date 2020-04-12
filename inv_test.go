package inv

import (
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	iq "github.com/rekki/go-query"
	"github.com/rekki/go-query/util/index"
)

// get full list from https://raw.githubusercontent.com/lutangar/cities.json/master/cities.json

type ExampleCity struct {
	ID      int32
	Name    string
	Country string
	Names   []string
}

func (e *ExampleCity) DocumentID() int32 {
	return e.ID
}

func (e *ExampleCity) IndexableFields() map[string][]string {
	out := map[string][]string{}

	out["name"] = []string{e.Name}
	out["names"] = e.Names
	out["country"] = []string{e.Country}

	return out
}

func toDocumentsID(in []*ExampleCity) []index.DocumentWithID {
	out := make([]index.DocumentWithID, len(in))
	for i, d := range in {
		out[i] = index.DocumentWithID(d)
	}
	return out
}

func TestExampleLite(t *testing.T) {
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := sql.Open("sqlite3", path.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := NewLiteIndex(db, SQLITE3, "test", nil)
	defer m.Close()

	list := []*ExampleCity{
		&ExampleCity{Name: "Amsterdam", Country: "NL", ID: 0},
		&ExampleCity{Name: "Amsterdam, USA", Country: "USA", ID: 1},
		&ExampleCity{Name: "London", Country: "UK", ID: 2},
		&ExampleCity{Name: "Sofia Amsterdam", Country: "BG", ID: 3},
	}
	err = m.Index(toDocumentsID(list)...)
	if err != nil {
		t.Fatal(err)
	}

	for i := len(list); i < 1000; i++ {
		//		list = append(list, &ExampleCity{Name: fmt.Sprintf("%dLondon", i), Country: "UK", ID: int32(i)})

		x := &ExampleCity{Name: "London", Country: "UK", ID: int32(i)}

		err = m.Index(index.DocumentWithID(x))
		if err != nil {
			t.Fatal(err)
		}

		list = append(list, x)
	}
	n := 0
	q := iq.And(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(q, func(did int32, score float32) {
		city := list[did]
		log.Printf("%v matching with score %f", city, score)
		n++
	})
	if n != 1 {
		t.Fatalf("expected 1 got %d", n)
	}

	n = 0
	qq := iq.Or(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(qq, func(did int32, score float32) {
		city := list[did]
		log.Printf("%v matching with score %f", city, score)
		n++
	})
	if n != 3 {
		t.Fatalf("expected 3 got %d", n)
	}

	n = 0
	qqq := iq.Or(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(qqq, func(did int32, score float32) {
		city := list[did]
		log.Printf("lazy %v matching with score %f", city, score)
		n++
	})
	if n != 3 {
		t.Fatalf("expected 3 got %d", n)
	}

	n = 0
	qqq = iq.Or(m.Terms("name", "london")...)

	m.Foreach(qqq, func(did int32, score float32) {
		n++
	})
	if n != 997 {
		t.Fatalf("expected 997 got %d", n)
	}

}

func BenchmarkDir(b *testing.B) {
	b.StopTimer()
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	idx := index.NewDirIndex(dir, index.NewFDCache(100), nil)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		docs := []index.DocumentWithID{}
		for j := 0; j < 10000; j++ {
			docs = append(docs, index.DocumentWithID(&ExampleCity{Name: "askjdaksdhkja dkj sakdhsa dlkjh", ID: int32(j * j)}))
		}
		err := idx.Index(docs...)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkSQL(b *testing.B) {
	b.StopTimer()
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		panic(err)
	}
	log.Printf("dir: %s", dir)
	//	defer os.RemoveAll(dir)

	db, err := sql.Open("sqlite3", path.Join(dir, "test.db"))
	if err != nil {
		panic(err)
	}
	idx, _ := NewLiteIndex(db, SQLITE3, "test", nil)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		docs := []index.DocumentWithID{}
		for j := 0; j < 10000; j++ {
			docs = append(docs, index.DocumentWithID(&ExampleCity{Name: "askjdaksdhkja dkj sakdhsa dlkjh", ID: int32(j * j)}))

		}
		err := idx.Index(docs...)
		if err != nil {
			panic(err)
		}

	}
	b.StopTimer()
}
