package db

import "os"

// diskNodeService represents a service that manages disk nodes
// associated with a file.
type diskNodeService struct {
	file *os.File
}

// newDiskNodeService creates a new diskNodeService instance.
// It takes an *os.File and returns a pointer to the diskNodeService.
func newDiskNodeService(file *os.File) *diskNodeService {
	return &diskNodeService{file: file}
}

// getRootNodeFromDisk retrieves the root node from the disk.
// It uses the BlockService to fetch the root block, then converts
// the block to a DiskNode and returns it. If an error occurs during
// fetching or conversion, it returns an error.
func (dns *diskNodeService) getRootNodeFromDisk() (*DiskNode, error) {
	bs := newBlockService(dns.file)     // Create a new BlockService with the file
	rootBlock, err := bs.getRootBlock() // Fetch the root block
	if err != nil {
		return nil, err // Return error if fetching the root block fails
	}
	return bs.convertBlockToDiskNode(rootBlock), nil // Convert the block and return the root node
}
