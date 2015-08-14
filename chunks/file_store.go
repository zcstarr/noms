package chunks

import (
	"bytes"
	"encoding/binary"
	"flag"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func (f FileStore) readRefMap() {
	for true {
		var s ref.Sha1Digest
		var i int64

		err := binary.Read(f.refMapFile, binary.LittleEndian, &s)
		if err != nil {
			return
		}
		err = binary.Read(f.refMapFile, binary.LittleEndian, &i)
		if err != nil {
			return
		}

		f.rm[s] = i
	}
}

func (f FileStore) appendRefMap(r ref.Ref, i int64) {
	err := binary.Write(f.refMapFile, binary.LittleEndian, r.Digest())
	d.Chk.NoError(err)
	err = binary.Write(f.refMapFile, binary.LittleEndian, i)
	d.Chk.NoError(err)
	f.rm[r.Digest()] = i
}

type FileStore struct {
	dir, root             string
	chunkFile, refMapFile *os.File
	rm                    map[ref.Sha1Digest]int64
	// For testing
	mkdirAll mkdirAllFn
}

type mkdirAllFn func(path string, perm os.FileMode) error

func NewFileStore(dir, root, refMap, chunks string) FileStore {
	d.Chk.NotEmpty(dir)
	d.Chk.NotEmpty(root)
	d.Chk.NotEmpty(refMap)
	d.Chk.NotEmpty(chunks)
	d.Chk.NoError(os.MkdirAll(dir, 0700))

	chunkFile, err := os.OpenFile(path.Join(dir, chunks), os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)
	d.Chk.NoError(err)

	refMapFile, err := os.OpenFile(path.Join(dir, refMap), os.O_APPEND|os.O_CREATE|os.O_RDWR, os.ModePerm)
	d.Chk.NoError(err)

	fs := FileStore{dir, path.Join(dir, root), chunkFile, refMapFile, make(map[ref.Sha1Digest]int64), os.MkdirAll}
	fs.readRefMap()
	return fs
}

func readRef(file *os.File) ref.Ref {
	s, err := ioutil.ReadAll(file)
	d.Chk.NoError(err)
	if len(s) == 0 {
		return ref.Ref{}
	}

	return ref.MustParse(string(s))
}

func (f FileStore) Root() ref.Ref {
	file, err := os.Open(f.root)
	if os.IsNotExist(err) {
		return ref.Ref{}
	}
	d.Chk.NoError(err)

	syscall.Flock(int(file.Fd()), syscall.LOCK_SH)
	defer file.Close()

	return readRef(file)
}

func (f FileStore) UpdateRoot(current, last ref.Ref) bool {
	file, err := os.OpenFile(f.root, os.O_RDWR|os.O_CREATE, os.ModePerm)
	d.Chk.NoError(err)
	syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	defer file.Close()

	existing := readRef(file)
	if existing != last {
		return false
	}

	file.Seek(0, 0)
	file.Truncate(0)
	file.Write([]byte(current.String()))

	f.chunkFile.Sync()
	f.refMapFile.Sync()
	return true
}

func (f FileStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	i, ok := f.rm[ref.Digest()]
	if !ok {
		return nil, nil
	}

	_, err := f.chunkFile.Seek(i, 0)
	d.Chk.NoError(err)

	var l int64
	binary.Read(f.chunkFile, binary.LittleEndian, &l)
	lr := io.LimitReader(f.chunkFile, l)
	return ioutil.NopCloser(lr), nil
}

func (f FileStore) Put() ChunkWriter {
	b := &bytes.Buffer{}
	h := ref.NewHash()
	return &fileChunkWriter{
		fs:       f,
		root:     f.dir,
		buffer:   b,
		writer:   io.MultiWriter(b, h),
		hash:     h,
		mkdirAll: f.mkdirAll,
	}
}

type fileChunkWriter struct {
	fs       FileStore
	root     string
	buffer   *bytes.Buffer
	writer   io.Writer
	hash     hash.Hash
	mkdirAll mkdirAllFn
}

func (w *fileChunkWriter) Write(data []byte) (int, error) {
	d.Chk.NotNil(w.buffer, "Write() cannot be called after Ref() or Close().")
	return w.writer.Write(data)
}

func (w *fileChunkWriter) Ref() (ref.Ref, error) {
	d.Chk.NoError(w.Close())
	return ref.FromHash(w.hash), nil
}

func (w *fileChunkWriter) Close() error {
	if w.buffer == nil {
		return nil
	}

	r := ref.FromHash(w.hash)
	if _, ok := w.fs.rm[r.Digest()]; ok {
		return nil
	}

	i, err := w.fs.chunkFile.Seek(0, 2)
	d.Chk.NoError(err)

	totalBytes := int64(w.buffer.Len())
	binary.Write(w.fs.chunkFile, binary.LittleEndian, totalBytes)
	written, err := io.Copy(w.fs.chunkFile, w.buffer)
	d.Chk.NoError(err)
	d.Chk.True(totalBytes == written, "Too few bytes written.") // BUG #83
	w.fs.appendRefMap(r, i)
	w.buffer = nil
	return nil
}

func getPath(root string, ref ref.Ref) string {
	s := ref.String()
	d.Chk.True(strings.HasPrefix(s, "sha1"))
	return path.Join(root, "sha1", s[5:7], s[7:9], s)
}

type fileStoreFlags struct {
	dir    *string
	root   *string
	refMap *string
	chunks *string
}

func fileFlags(prefix string) fileStoreFlags {
	return fileStoreFlags{
		flag.String(prefix+"fs", "", "directory to use for a file-based chunkstore"),
		flag.String(prefix+"fs-root", "root", "filename which holds the root ref in the filestore"),
		flag.String(prefix+"fs-refMap", "refMap", "filename which holds the ref offsets into the chunks file"),
		flag.String(prefix+"fs-chunks", "chunks", "filename which holds the chunk data"),
	}
}

func (f fileStoreFlags) createStore() ChunkStore {
	if *f.dir == "" || *f.root == "" {
		return nil
	} else {
		fs := NewFileStore(*f.dir, *f.root, *f.refMap, *f.chunks)
		return &fs
	}
}
