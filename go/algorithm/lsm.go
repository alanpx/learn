package algorithm

import (
    "unsafe"
    "os"
    "fmt"
    "bytes"
    "encoding/binary"
)

type LSM struct {
    *lsmConf
    mem *SkipList   // Memtable
    imem *SkipList  // Immutable Memtable
    tableId uint16  // increment 1 when producing new table
    manifest *lsmManifest
}
type lsmConf struct {
    Dir string
    BlockSize uint32
    MemSyncSize uint32
}
type lsmManifest struct {

}
type lsmKeyLenType uint16
type lsmValueLenType uint32

func NewLSM(conf *lsmConf) *LSM {
    lsm := LSM{}
    lsm.lsmConf = conf
    lsm.mem = NewSkipList()
    return &lsm
}
func (lsm *LSM) Add(key []byte, val []byte) bool {
    lenSize := int(unsafe.Sizeof(lsmKeyLenType(0)) + unsafe.Sizeof(lsmValueLenType(0)))
    size := lenSize + len(key) + len(val)
    if size > int(lsm.MemSyncSize) {
        return false
    }
    if lenSize * lsm.imem.total + lsm.mem.size + size > int(lsm.MemSyncSize) {
        lsm.imem = lsm.mem
        lsm.mem = NewSkipList()
    }
    lsm.mem.Add(key, val)
    return true
}
func (lsm *LSM) Get(key []byte) ([]byte, bool) {
    val, ok := lsm.mem.Get(key)
    if ok {
        return val, true
    }
}
func (lsm *LSM) syncImem() {
    // fileName format: d{level(uint16)}{tableId(uint16)}
    fileName := fmt.Sprintf("d%5d%5d", 0, lsm.tableId)
    file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        fmt.Println(err)
    }
    defer file.Close()
    buf := new(bytes.Buffer)
    node := lsm.imem.head
    for node != nil {
        binary.Write(buf, binary.LittleEndian, lsmKeyLenType(len(node.key)))
        binary.Write(buf, binary.LittleEndian, lsmValueLenType(len(node.val)))
        binary.Write(buf, binary.LittleEndian, node.key)
        binary.Write(buf, binary.LittleEndian, node.val)
    }
    file.Write(buf.Bytes())
}
