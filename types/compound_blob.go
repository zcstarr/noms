package types

import (
	"fmt"
	"io"
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// compoundBlob represents a list of Blobs.
// It implements the Blob interface.
type compoundBlob struct {
	compoundObject
}

func newCompoundBlob(offsets []uint64, futures []Future, cs chunks.ChunkSource) compoundBlob {
	return compoundBlob{compoundObject{offsets, futures, &ref.Ref{}, cs}}
}

// Reader implements the Blob interface
func (cb compoundBlob) Reader() io.ReadSeeker {
	return &compoundBlobReader{
		cb: cb}
}

type compoundBlobReader struct {
	leaves  chan blobLeaf
	curLeaf io.Reader
	eof     bool

	cb               compoundBlob
	currentReader    io.ReadSeeker
	currentBlobIndex int
	offset           int64
}

func (cbr *compoundBlobReader) Read(p []byte) (n int, err error) {
	fmt.Printf("Read on %p\n", cbr)
	if cbr.eof {
		fmt.Println("returning eof")
		return 0, io.EOF
	}

	if cbr.leaves == nil {
		cbr.Seek(0, 0)
	}

	read := func(r io.Reader) (exit bool) {
		l, e := r.Read(p[n:])
		n += l
		if e != io.EOF {
			err = e
			exit = true
		}
		return
	}

	if cbr.curLeaf != nil {
		fmt.Println("Got curLeaf")
		if read(cbr.curLeaf) {
			cbr.curLeaf = nil
			return
		}
	}

	for blobLeaf := range cbr.leaves {
		fmt.Printf("Got blobLeaf\n")
		cbr.curLeaf = blobLeaf.Reader()
		if read(cbr.curLeaf) {
			return
		}
	}

	fmt.Println("exited")

	cbr.eof = true
	return
}

// getLeaves writes all blobLeafs reachable from a blob to an output channel.
// TODO: Add a sem channel to control concurrency.
// TODO: Add a way to kill an in-process crawl (can just be closing the sem? - use a select with comma-ok to know when the sem is closed and break select. see http://stackoverflow.com/questions/13666253/breaking-out-of-a-select-statement-when-all-channels-are-closed)
func getLeaves(b compoundBlob, out chan<- blobLeaf) {
	fmt.Printf("getLeaves, %d\n", cap(out))
	in := make(chan chan blobLeaf, cap(out))

	go func() {
		for _, f := range b.futures {
			f := f
			ch := make(chan blobLeaf, cap(out))
			in <- ch
			fmt.Printf("getLeaves range\n")
			go func() {
				cb := f.Deref(b.cs).(Blob)
				switch cb := cb.(type) {
				case blobLeaf:
					ch <- cb
					fmt.Printf("getLeaves found blobLeaf\n")
					close(ch)
				case compoundBlob:
					fmt.Printf("getLeaves found compoundBlob\n")
					getLeaves(cb, ch)
				}
			}()
		}
		defer close(in)
	}()

	for ch := range in {
		for bl := range ch {
			fmt.Println("writing bl to flat channel")
			out <- bl
			fmt.Println("wrote bl to flat channel")
		}
	}

	close(out)
}

func (cbr *compoundBlobReader) Seek(offset int64, whence int) (int64, error) {
	// TODO: kill any previous read
	// TODO: support seeking

	cbr.curLeaf = nil
	cbr.leaves = make(chan blobLeaf, 4)
	go getLeaves(cbr.cb, cbr.leaves)
	return 0, nil
}

func (cbr *compoundBlobReader) findBlobOffset(abs uint64) int {
	return sort.Search(len(cbr.cb.offsets), func(i int) bool {
		return cbr.cb.offsets[i] > abs
	})
}

func (cbr *compoundBlobReader) updateReader() error {
	if cbr.currentBlobIndex < len(cbr.cb.futures) {
		v := cbr.cb.futures[cbr.currentBlobIndex].Deref(cbr.cb.cs)
		cbr.currentReader = v.(Blob).Reader()
	} else {
		cbr.currentReader = nil
	}
	return nil
}

func (cb compoundBlob) Ref() ref.Ref {
	return ensureRef(cb.ref, cb)
}

func (cb compoundBlob) Equals(other Value) bool {
	if other == nil {
		return false
	}
	return cb.Ref() == other.Ref()
}
