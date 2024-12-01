package db

import (
	"encoding/binary"
	"os"
)

const blockSize = 4096 // Fixed block size for storage on disk.
const maxLeafSize = 30 // Maximum number of leaf nodes allowed in a block.

// diskBlock represents a single block of data on the disk.
// It ensures that the block size does not exceed blockSize (4096 bytes).
type diskBlock struct {
	id                  uint64   // Unique ID of the block (8 bytes).
	currentLeafSize     uint64   // Number of data elements (leaf nodes) in the block (8 bytes).
	currentChildrenSize uint64   // Number of child block IDs in the block (8 bytes).
	childrenBlockIds    []uint64 // List of child block IDs (up to 30 entries).
	dataSet             []*pairs // List of data elements (pairs, up to 30 entries).
}

// blockService provides functionality to manage disk blocks.
// It allows reading, writing, and managing blocks stored in a file.
type blockService struct {
	file *os.File // File handle for the block storage file.
}

// isRootNode checks whether the given DiskNode is the root node.
// A root node is identified by its block ID being 0.
func (b *blockService) isRootNode(n *DiskNode) bool {
	return n.blockID == 0
}

// setData sets the data (pairs) for the diskBlock and updates its current leaf size.
func (b *diskBlock) setData(data []*pairs) {
	b.dataSet = data
	b.currentLeafSize = uint64(len(data))
}

// setChildren sets the child block IDs for the diskBlock and updates its current children size.
func (b *diskBlock) setChildren(childrenBlockIds []uint64) {
	b.childrenBlockIds = childrenBlockIds
	b.currentChildrenSize = uint64(len(childrenBlockIds))
}

// uint64ToBytes converts a uint64 value to a byte slice using little-endian encoding.
func uint64ToBytes(index uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, index)
	return b
}

// uint64FromBytes converts a byte slice to a uint64 value using little-endian encoding.
func uint64FromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// getLatestBlockID retrieves the ID of the most recently written block on disk.
// If the file is empty, it returns -1 to indicate that no blocks exist.
func (bs *blockService) getLatestBlockID() (int64, error) {
	fi, err := bs.file.Stat()
	if err != nil {
		return -1, err
	}

	length := fi.Size()
	if length == 0 {
		return -1, nil
	}
	// Calculate the ID of the last block based on file size and blockSize.
	return (int64(fi.Size()) / int64(blockSize)) - 1, nil
}

// getRootBlock retrieves the root block from disk. If no root block exists, it creates and initializes a new one.
func (bs *blockService) getRootBlock() (*diskBlock, error) {
	if !bs.rootBlockExists() {
		// Root block does not exist; create a new block.
		return bs.newBlock()
	}
	// Fetch the root block (block ID 0) from disk.
	return bs.getBlockFromDiskByBlockNumber(0)
}

// getBlockFromDiskByBlockNumber retrieves a block from disk using its block number.
// It calculates the byte offset and reads the block into memory.
func (bs *blockService) getBlockFromDiskByBlockNumber(index int64) (*diskBlock, error) {
	if index < 0 {
		panic("Index less than 0 requested")
	}
	offset := index * blockSize
	_, err := bs.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	blockBuffer := make([]byte, blockSize)
	_, err = bs.file.Read(blockBuffer)
	if err != nil {
		return nil, err
	}
	// Deserialize the block from the buffer.
	return bs.getBlockFromBuffer(blockBuffer), nil
}

// getBlockFromBuffer converts a byte slice (raw data) into a diskBlock structure.
func (bs *blockService) getBlockFromBuffer(blockBuffer []byte) *diskBlock {
	blockOffset := 0
	block := &diskBlock{}

	// Read block ID.
	block.id = uint64FromBytes(blockBuffer[blockOffset:])
	blockOffset += 8

	// Read current leaf size.
	block.currentLeafSize = uint64FromBytes(blockBuffer[blockOffset:])
	blockOffset += 8

	// Read current children size.
	block.currentChildrenSize = uint64FromBytes(blockBuffer[blockOffset:])
	blockOffset += 8

	// Read dataSet (list of pairs).
	block.dataSet = make([]*pairs, block.currentLeafSize)
	for i := 0; i < int(block.currentLeafSize); i++ {
		block.dataSet[i] = convertBytesToPair(blockBuffer[blockOffset:])
		blockOffset += pairSize
	}

	// Read children block IDs.
	block.childrenBlockIds = make([]uint64, block.currentChildrenSize)
	for i := 0; i < int(block.currentChildrenSize); i++ {
		block.childrenBlockIds[i] = uint64FromBytes(blockBuffer[blockOffset:])
		blockOffset += 8
	}
	return block
}

// getBufferFromBlock converts a diskBlock structure into a byte slice for storage.
func (bs *blockService) getBufferFromBlock(block *diskBlock) []byte {
	blockBuffer := make([]byte, blockSize)
	blockOffset := 0

	// Write block ID.
	copy(blockBuffer[blockOffset:], uint64ToBytes(block.id))
	blockOffset += 8

	// Write current leaf size.
	copy(blockBuffer[blockOffset:], uint64ToBytes(block.currentLeafSize))
	blockOffset += 8

	// Write current children size.
	copy(blockBuffer[blockOffset:], uint64ToBytes(block.currentChildrenSize))
	blockOffset += 8

	// Write dataSet (list of pairs).
	for i := 0; i < int(block.currentLeafSize); i++ {
		copy(blockBuffer[blockOffset:], convertPairsToBytes(block.dataSet[i]))
		blockOffset += pairSize
	}

	// Write children block IDs.
	for i := 0; i < int(block.currentChildrenSize); i++ {
		copy(blockBuffer[blockOffset:], uint64ToBytes(block.childrenBlockIds[i]))
		blockOffset += 8
	}
	return blockBuffer
}

// newBlock creates a new block on disk and assigns it a unique ID.
func (bs *blockService) newBlock() (*diskBlock, error) {
	latestBlockID, err := bs.getLatestBlockID()
	block := &diskBlock{}
	if err != nil {
		// If no blocks exist, this is the first block.
		block.id = 0
	} else {
		// Increment the latest block ID for the new block.
		block.id = uint64(latestBlockID) + 1
	}
	block.currentLeafSize = 0
	err = bs.writeBlockToDisk(block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

// writeBlockToDisk writes a block to its calculated position on disk.
func (bs *blockService) writeBlockToDisk(block *diskBlock) error {
	seekOffset := blockSize * block.id
	blockBuffer := bs.getBufferFromBlock(block)
	_, err := bs.file.Seek(int64(seekOffset), 0)
	if err != nil {
		return err
	}
	_, err = bs.file.Write(blockBuffer)
	if err != nil {
		return err
	}
	return nil
}

// convertDiskNodeToBlock converts a DiskNode structure into a diskBlock for storage.
func (bs *blockService) convertDiskNodeToBlock(node *DiskNode) *diskBlock {
	block := &diskBlock{id: node.blockID}
	tempElements := make([]*pairs, len(node.getElements()))
	for index, element := range node.getElements() {
		tempElements[index] = element
	}
	block.setData(tempElements)

	tempBlockIDs := make([]uint64, len(node.getChildBlockIDs()))
	for index, childBlockID := range node.getChildBlockIDs() {
		tempBlockIDs[index] = childBlockID
	}
	block.setChildren(tempBlockIDs)
	return block
}

// getNodeAtBlockID retrieves a DiskNode from its block ID.
func (bs *blockService) getNodeAtBlockID(blockID uint64) (*DiskNode, error) {
	block, err := bs.getBlockFromDiskByBlockNumber(int64(blockID))
	if err != nil {
		return nil, err
	}
	return bs.convertBlockToDiskNode(block), nil
}

// convertBlockToDiskNode converts a diskBlock into a DiskNode structure for in-memory operations.
func (bs *blockService) convertBlockToDiskNode(block *diskBlock) *DiskNode {
	node := &DiskNode{
		blockID:      block.id,
		blockService: bs,
		keys:         make([]*pairs, block.currentLeafSize),
	}
	for index := range node.keys {
		node.keys[index] = block.dataSet[index]
	}
	node.childrenBlockIDs = make([]uint64, block.currentChildrenSize)
	for index := range node.childrenBlockIDs {
		node.childrenBlockIDs[index] = block.childrenBlockIds[index]
	}
	return node
}

// saveNewNodeToDisk saves a new DiskNode as a block on disk.
// It assigns a new block ID to the node and writes the corresponding block to disk.
func (bs *blockService) saveNewNodeToDisk(n *DiskNode) error {
	// Get the latest block ID and assign the next ID to the new block.
	latestBlockID, err := bs.getLatestBlockID()
	if err != nil {
		return err
	}
	n.blockID = uint64(latestBlockID) + 1
	block := bs.convertDiskNodeToBlock(n)
	return bs.writeBlockToDisk(block)
}

// updateNodeToDisk updates an existing DiskNode's corresponding block on disk.
// It converts the DiskNode to a diskBlock and overwrites the existing block.
func (bs *blockService) updateNodeToDisk(n *DiskNode) error {
	block := bs.convertDiskNodeToBlock(n)
	return bs.writeBlockToDisk(block)
}

// updateRootNode updates the root node (block ID 0) on disk.
// It ensures that the root node always has a block ID of 0.
func (bs *blockService) updateRootNode(n *DiskNode) error {
	n.blockID = 0
	return bs.updateNodeToDisk(n)
}

// newBlockService initializes a new blockService with the provided file handle.
// The file is used to store and retrieve disk blocks.
func newBlockService(file *os.File) *blockService {
	return &blockService{file: file}
}

// rootBlockExists checks if the root block (block ID 0) exists on disk.
// It returns false if no blocks exist or if an error occurs while retrieving the latest block ID.
func (bs *blockService) rootBlockExists() bool {
	latestBlockID, err := bs.getLatestBlockID()
	if err != nil {
		// If an error occurs or the file is empty, assume the root block does not exist.
		return false
	} else if latestBlockID == -1 {
		// If the latest block ID is -1, the file is empty, and no blocks exist.
		return false
	}
	return true
}

// getMaxLeafSize dynamically calculates and returns the maximum number of leaf nodes allowed in a block.
// Currently, this function returns the constant maxLeafSize but could be extended to calculate dynamically.
func (bs *blockService) getMaxLeafSize() int {
	return maxLeafSize
}
