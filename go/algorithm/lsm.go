package algorithm

import (
    "unsafe"
    "os"
    "fmt"
    "bytes"
    "encoding/binary"
    "math"
    "io/ioutil"
)

type LSM struct {
    *lsmConf
    mem *SkipList   // Memtable
    imem *SkipList  // Immutable Memtable
    tableId uint16  // increment 1 when producing new table
    manifest []*lsmManifest // manifest for every level
    memSize uint32
}
type lsmConf struct {
    Dir string
    BlockSize uint32
    MemSyncSize uint32
}
type lsmManifest struct {
    level uint16
    fileCount uint16
    fileName []string
    minKey [][]byte
    maxKey [][]byte
    dataBlockCount []uint32
}
type lsmKeyLenType uint16
type lsmValueLenType uint32
type lsmRecordLenType uint32
type recordType byte
const (
    _ recordType = iota
    recordFull
    recordFirst
    recordMiddle
    recordLast
)

func NewLSM(conf *lsmConf) *LSM {
    lsm := LSM{}
    lsm.lsmConf = conf
    lsm.mem = NewSkipList()
    lsm.readManifest()
    return &lsm
}
func (lsm *LSM) Add(key []byte, val []byte) bool {
    eleSize := len(key) + len(val)
    if eleSize > int(lsm.MemSyncSize) {
        return false
    }
    if eleSize + lsm.mem.size >= int(lsm.MemSyncSize) {
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
    if len(lsm.manifest) == 0 {
        return nil, false
    }
    for i := 0; i < len(lsm.manifest); i++ {
        m := lsm.manifest[i]
        for j := 0; j < int(m.fileCount); j++ {
            if bytes.Compare(key, m.minKey[j]) >= 0 && bytes.Compare(key, m.maxKey[j]) <= 0 {
                val, ok := lsm.getFromSSTable(key, m.fileName[j])
                if ok {
                    return val, ok
                }
            }
        }
    }
    return nil ,false
}
func (lsm *LSM) getFromSSTable(key []byte, fileName string) ([]byte, bool) {
    return nil, false
}
func (lsm *LSM) syncImem() {
    /*
     * recordFull   lsmRecordLenType lsmKeyLenType key val
     * recordFirst  lsmRecordLenType lsmKeyLenType key val
     * recordMiddle lsmRecordLenType val
     * recordLast   lsmRecordLenType val
     */
    buf := new(bytes.Buffer)
    node := lsm.imem.head
    var minKey, maxKey []byte
    minKey = node.key
    size := 0
    spare := int(lsm.BlockSize)
    prefixSize := int(unsafe.Sizeof(recordType(0)) + unsafe.Sizeof(lsmRecordLenType(0)))
    var minKeySlice [][]byte
    for node != nil {
        eleBuf := new(bytes.Buffer)
        binary.Write(eleBuf, binary.LittleEndian, lsmKeyLenType(len(node.key)))
        binary.Write(eleBuf, binary.LittleEndian, node.key)
        binary.Write(eleBuf, binary.LittleEndian, node.val)
        if eleBuf.Len() <= spare - prefixSize {
            binary.Write(buf, binary.LittleEndian, recordFull)
            binary.Write(buf, binary.LittleEndian, lsmRecordLenType(eleBuf.Len()))
            binary.Write(buf, binary.LittleEndian, eleBuf.Bytes())
            size += prefixSize + eleBuf.Len()
        } else {
            hasFirst := false
            if prefixSize < spare {
                binary.Write(buf, binary.LittleEndian, recordFirst)
                binary.Write(buf, binary.LittleEndian, lsmRecordLenType(spare - prefixSize))
                data := make([]byte, spare - prefixSize)
                eleBuf.Read(data)
                binary.Write(buf, binary.LittleEndian, data)
                hasFirst = true
            } else {
                binary.Write(buf, binary.LittleEndian, make([]byte, spare))
            }
            size += spare
            spare = int(lsm.BlockSize)
            for eleBuf.Len() > 0 {
                if !hasFirst {
                    binary.Write(buf, binary.LittleEndian, recordFirst)
                } else if eleBuf.Len() <= spare - prefixSize {
                    binary.Write(buf, binary.LittleEndian, recordLast)
                } else {
                    binary.Write(buf, binary.LittleEndian, recordMiddle)
                }
                binary.Write(buf, binary.LittleEndian, lsmRecordLenType(spare - prefixSize))
                data := make([]byte, spare - prefixSize)
                l, _ := eleBuf.Read(data)
                binary.Write(buf, binary.LittleEndian, data[0:l])
                size += prefixSize + l
            }
        }

        spare = size % int(lsm.BlockSize)
        if spare == 0 {
            spare = int(lsm.BlockSize)
        }

        if node.next[0] == nil {
            maxKey = node.key
        }
        node = node.next[0]
    }

    // fileName format: d{level(uint16)}{tableId(uint16)}
    fileName := fmt.Sprintf("%s/D%5d%5d", lsm.Dir, 0, lsm.tableId)
    file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()
    file.Write(buf.Bytes())

    if len(lsm.manifest) == 0 {
        lsm.manifest = make([]*lsmManifest, 1)
        lsm.manifest[0] = &lsmManifest{}
    }
    lsm.manifest[0].fileCount++
    lsm.manifest[0].fileName = append([]string{ fileName }, lsm.manifest[0].fileName...)
    lsm.manifest[0].minKey = append([][]byte{ minKey }, lsm.manifest[0].minKey...)
    lsm.manifest[0].maxKey = append([][]byte{ maxKey }, lsm.manifest[0].maxKey...)
    blockCount := uint32(math.Ceil(float64(size/int(lsm.BlockSize))))
    lsm.manifest[0].dataBlockCount = append([]uint32{ blockCount }, lsm.manifest[0].dataBlockCount...)
}
func (lsm *LSM) syncManifest() {
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, uint16(len(lsm.manifest)))
    for i := 0; i < len(lsm.manifest); i++ {
        m := lsm.manifest[i]
        binary.Write(buf, binary.LittleEndian, m.level)
        binary.Write(buf, binary.LittleEndian, m.fileCount)
        for j := 0; j < int(m.fileCount); j++ {
            binary.Write(buf, binary.LittleEndian, lsmKeyLenType(len(m.minKey[j])))
            binary.Write(buf, binary.LittleEndian, m.minKey[j])
            binary.Write(buf, binary.LittleEndian, lsmKeyLenType(len(m.maxKey[j])))
            binary.Write(buf, binary.LittleEndian, m.maxKey[j])
            binary.Write(buf, binary.LittleEndian, m.dataBlockCount[j])
        }
    }

    fileName := fmt.Sprintf("%s/M", lsm.Dir)
    file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()
    file.Write(buf.Bytes())
}
func (lsm *LSM) readManifest() {
    fileName := fmt.Sprintf("%s/M", lsm.Dir)
    file, err := os.Open(fileName)
    if err != nil {
        return
    }
    defer file.Close()

    data, err := ioutil.ReadAll(file)
    if err != nil {
        fmt.Println(err)
        return
    }

    reader := bytes.NewReader(data)
    var levelCount uint16
    binary.Read(reader, binary.LittleEndian, &levelCount)
    manifest := make([]*lsmManifest, levelCount)
    for i := 0; i < int(levelCount); i++ {
        m := lsmManifest{}
        binary.Read(reader, binary.LittleEndian, &m.level)
        binary.Read(reader, binary.LittleEndian, &m.fileCount)
        for j := 0; j < int(m.fileCount); j++ {
            m.minKey = make([][]byte, m.fileCount)
            m.maxKey = make([][]byte, m.fileCount)
            m.dataBlockCount = make([]uint32, m.fileCount)

            var keyLen lsmKeyLenType
            binary.Read(reader, binary.LittleEndian, &keyLen)
            m.minKey[j] = make([]byte, keyLen)
            binary.Read(reader, binary.LittleEndian, &m.minKey[j])

            binary.Read(reader, binary.LittleEndian, &keyLen)
            m.maxKey[j] = make([]byte, keyLen)
            binary.Read(reader, binary.LittleEndian, &m.maxKey[j])

            binary.Read(reader, binary.LittleEndian, &m.dataBlockCount[j])
        }
    }
    lsm.manifest = manifest
}
