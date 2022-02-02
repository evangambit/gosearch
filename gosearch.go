package search

import (
  "log"
  "database/sql"
)

var kFetchSize int = 10

type TokenIterator struct {
  TagId int
  DocIds []int
  Offset int
  Delta int
  Db *sql.DB
}

type SearchResults struct {
  DocIds []int
  Offsets []int
  Done bool
}

var kLastDocid int = 9999

func AtLeast(iters []TokenIterator, k int, limit int) []int {
  results := []int{}

  vals := []int{}
  for i, _ := range iters {
    vals = append(vals, next(&iters[i]))
  }

  for {
    minval := min_array(vals)
    if minval == kLastDocid {
      return results
    }
    if num_equal(vals, minval) >= k {
      results = append(results, minval)
    }
    for i, v := range vals {
      if v == minval {
        vals[i] = next(&iters[i])
      }
    }
    if len(results) >= limit {
      return results
    }
  }

  return results
}

func num_equal(array []int, value int) int {
  r := 0
  for _, val := range array {
    if val == value {
      r += 1
    }
  }
  return r
}

func min(a int, b int) int {
    if a < b {
        return a
    }
    return b
}

func min_array(array []int) int {
  r := array[0]
  for _, val := range array {
    r = min(r, val)
  }
  return r
}


func fetch(self *TokenIterator, n int) {
  rows, err := self.Db.Query(`
    SELECT docid
    FROM doctags
    WHERE tagid = ?
    LIMIT ?
    OFFSET ?`,
    self.TagId,
    n,
    self.Offset + len(self.DocIds),
  )
  if err != nil {
    log.Fatal(err)
  }
  for rows.Next() {
    var docid int
    rows.Scan(&docid)
    self.DocIds = append(self.DocIds, docid)
  }
}

func next(self *TokenIterator) int {
  if self.Delta + 1 >= len(self.DocIds) {
    fetch(self, kFetchSize)
  }
  self.Delta += 1
  if self.Delta >= len(self.DocIds) {
    return kLastDocid
  }
  return self.DocIds[self.Delta]
}

func Search(db *sql.DB, tagIds []int, offsets []int, k int, limit int) SearchResults {
  // log.Printf("offsets = ", offsets)
  iters := []TokenIterator{}
  for i, tagId := range tagIds {
    ti := TokenIterator{}
    ti.TagId = tagId
    ti.DocIds = nil
    ti.Offset = offsets[i]
    ti.Delta = 0
    ti.Db = db
    fetch(&ti, kFetchSize)
    ti.Delta = -1
    iters = append(iters, ti)
  }

  results := SearchResults{
    []int{},
    []int{},
    false,
  }

  results.DocIds = AtLeast(iters, k, limit)
  if len(results.DocIds) < limit {
    results.Done = true;
  }

  for _, iter := range(iters) {
    results.Offsets = append(results.Offsets, iter.Offset + iter.Delta)
  }

  return results  
}
