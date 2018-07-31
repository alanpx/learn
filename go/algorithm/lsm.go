package algorithm

import (
    "unsafe"
    "os"
    "fmt"
    "bytes"
    "encoding/binary"
    "math"
    "io/ioutil"
    "time"
)

type LSM struct {
    *LSMConf
    mem      *SkipList      // Memtable
    imem     *SkipList      // Immutable Memtable
    manifest []*lsmManifest // manifest for every level
    memSize  uint32
    tableId  uint16
}
type LSMConf struct {
    Dir         string
    BlockSize   uint32
    MemSyncSize uint32
}
type lsmManifest struct {
    fileCount      uint16
    fileName       [][]byte
    minKey         [][]byte
    maxKey         [][]byte
    dataBlockCount []uint32
}
type lsmKeyLenType uint16
type lsmOperType byte
type lsmRecordLenType uint32
type lsmRecordType byte

const (
    _               lsmRecordType = iota
    lsmRecordFull
    lsmRecordFirst
    lsmRecordMiddle
    lsmRecordLast
)
const (
    lsmOperAdd lsmOperType = iota
    lsmOperDel
)

func NewLSM(conf *LSMConf) *LSM {
    lsm := LSM{}
    lsm.LSMConf = conf
    lsm.mem = NewSkipList()
    lsm.readManifest()
    return &lsm
}
func (lsm *LSM) Add(key []byte, val []byte) bool {
    val = append([]byte{byte(lsmOperAdd)}, val...)
    return lsm.add(key, val)
}
func (lsm *LSM) Rem(key []byte) bool {
    val := []byte{byte(lsmOperDel)}
    return lsm.add(key, val)
}
func (lsm *LSM) add(key []byte, val []byte) bool {
    eleSize := len(key) + len(val)
    if eleSize > int(lsm.MemSyncSize) {
        return false
    }
    if eleSize+lsm.mem.size >= int(lsm.MemSyncSize) {
        lsm.imem = lsm.mem
        lsm.mem = NewSkipList()
        lsm.syncImem()
    }
    lsm.mem.AddOrUpdate(key, val)
    return true
}
func (lsm *LSM) Get(key []byte) ([]byte, bool) {
    val, ok := lsm.mem.Get(key)
    if ok {
        if val[0] == byte(lsmOperAdd) {
            return val[1:], ok
        } else if val[0] == byte(lsmOperDel) {
            return nil, false
        }
    }
    if len(lsm.manifest) == 0 {
        return nil, false
    }
    for i := 0; i < len(lsm.manifest); i++ {
        m := lsm.manifest[i]
        for j := 0; j < int(m.fileCount); j++ {
            if bytes.Compare(key, m.minKey[j]) >= 0 && bytes.Compare(key, m.maxKey[j]) <= 0 {
                val, ok := lsm.getFromSSTable(key, lsm.Dir+string(m.fileName[j]), m.dataBlockCount[j])
                if ok {
                    if val[0] == byte(lsmOperAdd) {
                        return val[1:], ok
                    } else if val[0] == byte(lsmOperDel) {
                        return nil, false
                    }
                }
            }
        }
    }
    return nil, false
}
func (lsm *LSM) getFromSSTable(key []byte, fileName string, dataBlockCount uint32) ([]byte, bool) {
    file, err := os.Open(fileName)
    if err != nil {
        return nil, false
    }
    defer file.Close()

    // read index block, to find the target data block
    index := make([]byte, lsm.BlockSize)
    file.ReadAt(index, int64(dataBlockCount*lsm.BlockSize))
    indexReader := bytes.NewReader(index)
    maxKey := make([][]byte, dataBlockCount)
    var keyLen keyLenType
    i := 0
    for i = 0; i < len(maxKey); i++ {
        binary.Read(indexReader, binary.LittleEndian, &keyLen)
        maxKey[i] = make([]byte, keyLen)
        if keyLen > 0 {
            binary.Read(indexReader, binary.LittleEndian, &maxKey[i])
        }
        if bytes.Compare(key, maxKey[i]) <= 0 {
            break
        }
    }

    data := make([]byte, lsm.BlockSize)
    file.ReadAt(data, int64(i*int(lsm.BlockSize)))
    dataReader := bytes.NewReader(data)
    var recordType lsmRecordType
    var recordLen lsmRecordLenType
    var val []byte
    for dataReader.Len() > 0 {
        binary.Read(dataReader, binary.LittleEndian, &recordType)
        binary.Read(dataReader, binary.LittleEndian, &recordLen)
        if recordType == lsmRecordFirst || recordType == lsmRecordFull {
            var keyLen lsmKeyLenType
            binary.Read(dataReader, binary.LittleEndian, &keyLen)
            k := make([]byte, keyLen)
            binary.Read(dataReader, binary.LittleEndian, &k)
            valLen := int(recordLen) - int(unsafe.Sizeof(lsmKeyLenType(0))) - int(keyLen)
            v := make([]byte, valLen)
            binary.Read(dataReader, binary.LittleEndian, &v)
            if bytes.Compare(key, k) == 0 {
                val = append(val, v...)
                if recordType == lsmRecordFull {
                    return val, true
                } else {
                    data = make([]byte, lsm.BlockSize)
                    file.ReadAt(data, int64((i+1)*int(lsm.BlockSize)))
                    dataReader = bytes.NewReader(data)
                }
            }
        } else if recordType == lsmRecordMiddle || recordType == lsmRecordLast {
            v := make([]byte, recordLen)
            binary.Read(dataReader, binary.LittleEndian, &v)
            if len(val) > 0 {
                val = append(val, v...)
                if recordType == lsmRecordLast {
                    return val, true
                }
            }
        } else {
            // the rest space is not enough for a record
            break
        }
    }

    return nil, false
}
func (lsm *LSM) SyncAll() {
    if lsm.imem == nil && lsm.mem != nil && lsm.mem.total > 0 {
        lsm.imem = lsm.mem
        lsm.mem = NewSkipList()
    }
    lsm.syncImem()
    lsm.syncManifest()
}
func (lsm *LSM) syncImem() {
    if lsm.imem == nil {
        return
    }

    /*
     * Every data block should begin with a record(otherwise it can not be parsed individually).
     * If the spare space is not enough for the record prefix and the key, the record will be put to the next block(for the sake of parsing).
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
    prefixSize := int(unsafe.Sizeof(lsmRecordType(0)) + unsafe.Sizeof(lsmRecordLenType(0)))
    maxKeyMap := make(map[int][]byte)
    block := 0
    for node != nil {
        eleBuf := new(bytes.Buffer)
        binary.Write(eleBuf, binary.LittleEndian, lsmKeyLenType(len(node.key)))
        binary.Write(eleBuf, binary.LittleEndian, node.key)
        binary.Write(eleBuf, binary.LittleEndian, node.val)

        if eleBuf.Len() <= spare-prefixSize {
            binary.Write(buf, binary.LittleEndian, lsmRecordFull)
            binary.Write(buf, binary.LittleEndian, lsmRecordLenType(eleBuf.Len()))
            binary.Write(buf, binary.LittleEndian, eleBuf.Bytes())
            block = size / int(lsm.BlockSize)
            maxKeyMap[block] = node.key
            size += prefixSize + eleBuf.Len()
        } else {
            if prefixSize+int(unsafe.Sizeof(lsmKeyLenType(0)))+len(node.key) < spare {
                binary.Write(buf, binary.LittleEndian, lsmRecordFirst)
                binary.Write(buf, binary.LittleEndian, lsmRecordLenType(spare-prefixSize))
                data := make([]byte, spare-prefixSize)
                eleBuf.Read(data)
                binary.Write(buf, binary.LittleEndian, data)
                block = size / int(lsm.BlockSize)
                maxKeyMap[block] = node.key
                size += spare
                spare = int(lsm.BlockSize)

                for eleBuf.Len() > 0 {
                    l := 0
                    if eleBuf.Len() <= spare-prefixSize {
                        binary.Write(buf, binary.LittleEndian, lsmRecordLast)
                        l = eleBuf.Len()
                    } else {
                        binary.Write(buf, binary.LittleEndian, lsmRecordMiddle)
                        l = spare - prefixSize
                    }
                    binary.Write(buf, binary.LittleEndian, lsmRecordLenType(l))
                    data := make([]byte, l)
                    eleBuf.Read(data)
                    binary.Write(buf, binary.LittleEndian, data)
                    size += prefixSize + l
                }
            } else {
                binary.Write(buf, binary.LittleEndian, make([]byte, spare))
                size += spare
                spare = int(lsm.BlockSize)
                continue
            }
        }

        spare = int(lsm.BlockSize) - size%int(lsm.BlockSize)
        if spare == 0 {
            spare = int(lsm.BlockSize)
        }

        if node.next[0] == nil {
            maxKey = node.key
        }
        node = node.next[0]
    }

    if size%int(lsm.BlockSize) > 0 {
        binary.Write(buf, binary.LittleEndian, make([]byte, int(lsm.BlockSize)-size%int(lsm.BlockSize)))
    }
    blockCount := uint32(math.Ceil(float64(size) / float64(lsm.BlockSize)))

    // index block
    maxKeySlice := make([][]byte, blockCount)
    for k, v := range maxKeyMap {
        maxKeySlice[k] = v
    }
    for _, v := range maxKeySlice {
        if v == nil {
            binary.Write(buf, binary.LittleEndian, lsmKeyLenType(0))
        } else {
            binary.Write(buf, binary.LittleEndian, lsmKeyLenType(len(v)))
            binary.Write(buf, binary.LittleEndian, v)
        }
    }

    fileName := fmt.Sprintf("D%d-%d-%d", 0, time.Now().Unix(), lsm.tableId)
    lsm.tableId++
    file, err := os.OpenFile(lsm.Dir+fileName, os.O_WRONLY|os.O_CREATE, 0666)
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
    lsm.manifest[0].fileName = append([][]byte{[]byte(fileName)}, lsm.manifest[0].fileName...)
    lsm.manifest[0].minKey = append([][]byte{minKey}, lsm.manifest[0].minKey...)
    lsm.manifest[0].maxKey = append([][]byte{maxKey}, lsm.manifest[0].maxKey...)
    lsm.manifest[0].dataBlockCount = append([]uint32{blockCount}, lsm.manifest[0].dataBlockCount...)

    lsm.imem = nil
}
func (lsm *LSM) syncManifest() {
    // levelCount(uint16) nextTableId(uint16) [fileCount(uint16) [fileNameLen(uint16) fileName([]byte) keyLen(lsmKeyLenType) minKey([]byte) keyLen(lsmKeyLenType) maxKey([]byte) dataBlockCount(uint32)]]
    buf := new(bytes.Buffer)
    binary.Write(buf, binary.LittleEndian, uint16(len(lsm.manifest)))
    binary.Write(buf, binary.LittleEndian, lsm.tableId)
    for i := 0; i < len(lsm.manifest); i++ {
        m := lsm.manifest[i]
        binary.Write(buf, binary.LittleEndian, m.fileCount)
        for j := 0; j < int(m.fileCount); j++ {
            binary.Write(buf, binary.LittleEndian, uint16(len(m.fileName[j])))
            binary.Write(buf, binary.LittleEndian, m.fileName[j])
            binary.Write(buf, binary.LittleEndian, lsmKeyLenType(len(m.minKey[j])))
            binary.Write(buf, binary.LittleEndian, m.minKey[j])
            binary.Write(buf, binary.LittleEndian, lsmKeyLenType(len(m.maxKey[j])))
            binary.Write(buf, binary.LittleEndian, m.maxKey[j])
            binary.Write(buf, binary.LittleEndian, m.dataBlockCount[j])
        }
    }

    fileName := lsm.Dir + "M"
    file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()
    file.Write(buf.Bytes())
}
func (lsm *LSM) readManifest() {
    fileName := lsm.Dir + "M"
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
    binary.Read(reader, binary.LittleEndian, &lsm.tableId)
    manifest := make([]*lsmManifest, levelCount)
    for i := 0; i < int(levelCount); i++ {
        m := lsmManifest{}
        manifest[i] = &m
        binary.Read(reader, binary.LittleEndian, &m.fileCount)
        m.fileName = make([][]byte, m.fileCount)
        m.minKey = make([][]byte, m.fileCount)
        m.maxKey = make([][]byte, m.fileCount)
        m.dataBlockCount = make([]uint32, m.fileCount)

        for j := 0; j < int(m.fileCount); j++ {
            var fileNameLen uint16
            binary.Read(reader, binary.LittleEndian, &fileNameLen)
            m.fileName[j] = make([]byte, fileNameLen)
            binary.Read(reader, binary.LittleEndian, &m.fileName[j])

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
