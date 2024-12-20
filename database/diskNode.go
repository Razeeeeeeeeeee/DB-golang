package db

import (
	"fmt"
	"sync"
)

/**
* Insertion Algorithm
1. It will begin from root and value will always be inserted into a leaf node
2. Insert Function Begin
3. If current node is leaf node, then return pick current node, Current Node Insertion Algorithm
    1. This section gives 3 outputs, 1 middle element and 2 child nodes or null,null,null
    2. Insert into the current node
    3. If its full, then sort it and make two new child nodes without the middle node ( NODE CREATION WILL TAKE PLACE HERE)
    4. take out the middle element along with the two child nodes,  Leaf Splitting no children Algorithm:
        1. Pick middle element by using length of array/2, lets say its index i
        2. Club all elements from 0 to i-1, and i+1 to len(array) and create new seperate nodes by inserting these 2 arrays into the respective keys[] of respective nodes
        3. Since the current node is a leaf node, we do not need to worry about its children and we can leave them to be null for both
        4. return middle,leftNode,rightNode
    5. If its not full, then return null,null,null
4. If this is not a leaf node, then find out the proper child node, Child Node Searching Algorithm:
    1. Input : Value to be inserted, the current Node. Output : Pointer to the childnode
    2. Since the list of values/elements is sorted, perform a binary or linear search to find the first element greater than the value to be inserted, if such an element is found, return pointer at position i, else return last pointer ( ie. the last pointer)
5. After getting the pointer to that element, call insert function Step 2 on that node RECURSIVELY ONLY HERE
6. If we get output from child node insert function Step 2, then this means that we have to insert the middle element received and accomodate the 2 pointers in the current node as well  discarding the old pointer ( NODE DESTRUCTION WILL ONLY TAKE PLACE HERE )
    1. If we got null as output then do nothing, else
    2. Insert into current Node, Popped up element and two child pointers insertion algorithm, Popped Up Joining Algorithm:
        1. Insert element and sort the array
        2. Now we need to discard 1 child pointer and insert 2 child pointers, Child Pointer Manipulation Algorithm :
        3. Find index of inserted element in array, lets say that it is i
        4. Now in the child pointer array, insert the left and right pointers at ith and i+1 th index
    3. If its full, sort it and make two new child nodes, Leaf Splitting with children Algorithm:
        1. Pick middle element by using length of array/2, lets say its index i (Same as 3.4.1)
        2. Club all elements from 0 to i-1, and i+1 to len(lkeys array) and create new seperate nodes by inserting these 2 arrays into the respective keys[] of respective nodes (Same as 3.4.2)
        3. For children[], split the current node's children array into 2 parts, part1 will be from 0 to i, and part 2 will be from i+1 to len(children array), and insert them into leftNode children, and rightNode children
        4. If current node is not the root node return middle,leftNode,rightNode
        5. else if current node == rootNode, Root Node Splitting Algorithm:
            1. Create a new node with elements array as keys[0] = middle
            2. children[0]=leftNode and children[1]=rightNode
            3. Set btree.root=new node
            4. return null,null,null

*/

// DiskNode represents an in-memory node in the B-tree.
type DiskNode struct {
	// keys stores the key-value pairs in the node.
	keys []*pairs
	// childrenBlockIDs stores the block IDs of child nodes.
	childrenBlockIDs []uint64
	// blockID is the block ID of the current node.
	blockID uint64
	// blockService provides an interface to interact with the block storage.
	blockService *blockService
	// mu is a read-write mutex for synchronizing access to the node.
	mu sync.RWMutex
}

// isLeaf checks if the current node is a leaf node.
func (n *DiskNode) isLeaf() bool {
	return len(n.childrenBlockIDs) == 0
}

// PrintTree traverses and prints the entire tree rooted at the current node.
func (n *DiskNode) printTree(level int) {
	currentLevel := level
	if level == 0 {
		currentLevel = 1
	}

	n.printNode()
	for i := 0; i < len(n.childrenBlockIDs); i++ {
		fmt.Println("Printing ", i+1, " th child of level : ", currentLevel)
		childNode, err := n.getChildAtIndex(i)
		if err != nil {
			panic(err)
		}
		childNode.printTree(currentLevel + 1)
	}
}

/**
* Do a linear search and insert the element
 **/
// addElement inserts an element into the node's key array while maintaining sorted order.
func (n *DiskNode) addElement(element *pairs) int {
	elements := n.getElements()
	indexForInsertion := 0
	elementInsertedInBetween := false
	for i := 0; i < len(elements); i++ {
		if elements[i].key >= element.key {
			// We have found the right place to insert the element

			indexForInsertion = i
			elements = append(elements, nil)
			copy(elements[indexForInsertion+1:], elements[indexForInsertion:])
			elements[indexForInsertion] = element
			n.setElements(elements)
			elementInsertedInBetween = true
			break
		}
	}
	if !elementInsertedInBetween {
		// If we are here, it means we need to insert the element at the rightmost position
		n.setElements(append(elements, element))
		indexForInsertion = len(n.getElements()) - 1
	}

	return indexForInsertion
}

// hasOverFlown checks if the node has exceeded its maximum capacity.
func (n *DiskNode) hasOverFlown() bool {
	return len(n.getElements()) > n.blockService.getMaxLeafSize()
}

// getElements returns the key-value pairs stored in the node.
func (n *DiskNode) getElements() []*pairs {
	return n.keys
}

// setElements updates the key-value pairs stored in the node.
func (n *DiskNode) setElements(newElements []*pairs) {
	n.keys = newElements
}

// getElementAtIndex returns the key-value pair at the specified index.
func (n *DiskNode) getElementAtIndex(index int) *pairs {
	return n.keys[index]
}

// getChildAtIndex returns the child node at the specified index.
func (n *DiskNode) getChildAtIndex(index int) (*DiskNode, error) {
	return n.blockService.getNodeAtBlockID(n.childrenBlockIDs[index])
}

// shiftRemainingChildrenToRight shifts the child nodes to the right starting from the specified index.
func (n *DiskNode) shiftRemainingChildrenToRight(index int) {
	if len(n.childrenBlockIDs) < index+1 {
		// This means index is the last element, hence no need to shift
		return
	}
	n.childrenBlockIDs = append(n.childrenBlockIDs, 0)
	copy(n.childrenBlockIDs[index+1:], n.childrenBlockIDs[index:])
	n.childrenBlockIDs[index] = 0
}

// setChildAtIndex updates the child node at the specified index.
func (n *DiskNode) setChildAtIndex(index int, childNode *DiskNode) {
	if len(n.childrenBlockIDs) < index+1 {
		n.childrenBlockIDs = append(n.childrenBlockIDs, 0)
	}
	n.childrenBlockIDs[index] = childNode.blockID
}

// getLastChildNode returns the last child node.
func (n *DiskNode) getLastChildNode() (*DiskNode, error) {
	return n.getChildAtIndex(len(n.childrenBlockIDs) - 1)
}

// getChildNodes returns all child nodes.
func (n *DiskNode) getChildNodes() ([]*DiskNode, error) {
	childNodes := make([]*DiskNode, len(n.childrenBlockIDs))
	for index := range n.childrenBlockIDs {
		childNode, err := n.getChildAtIndex(index)
		if err != nil {
			return nil, err
		}
		childNodes[index] = childNode
	}
	return childNodes, nil
}

func (n *DiskNode) getChildBlockIDs() []uint64 {
	return n.childrenBlockIDs
}

func (n *DiskNode) printNode() {
	fmt.Println("Printing Node")
	fmt.Println("--------------")
	for i := 0; i < len(n.getElements()); i++ {
		fmt.Println(n.getElementAtIndex(i))
	}
	fmt.Println("**********************")
}

// splitLeafNode splits a leaf node into two child nodes when it overflows.
func (n *DiskNode) splitLeafNode() (*pairs, *DiskNode, *DiskNode, error) {
	/**
		LEAF SPLITTING WITHOUT CHILDREN ALGORITHM
				If its full, then  make two new child nodes without the middle node ( NODE CREATION WILL TAKE PLACE HERE)
	    		Take out the middle element along with the two child nodes,  Leaf Splitting no children Algorithm:
	        	1. Pick middle element by using length of array/2, lets say its index i
	        	2. Club all elements from 0 to i-1, and i+1 to len(array) and create new seperate nodes by inserting these 2 arrays into the respective keys[] of respective nodes
	        	3. Since the current node is a leaf node, we do not need to worry about its children and we can leave them to be null for both
	        	4. return middle,leftNode,rightNode
	*/
	elements := n.getElements()
	midIndex := len(elements) / 2
	middle := elements[midIndex]

	// Now lets split elements array into 2 as we are splitting this node
	elements1 := elements[0:midIndex]
	elements2 := elements[midIndex+1:]

	// Now lets construct new Nodes from these 2 element arrays
	leftNode, err := newLeafNode(elements1, n.blockService)
	if err != nil {
		return nil, nil, nil, err
	}
	rightNode, err := newLeafNode(elements2, n.blockService)
	if err != nil {
		return nil, nil, nil, err
	}
	return middle, leftNode, rightNode, nil
}

// splitNonLeafNode splits a non-leaf node into two child nodes when it overflows.
func (n *DiskNode) splitNonLeafNode() (*pairs, *DiskNode, *DiskNode, error) {
	/**
		NON-LEAF NODE SPLITTING ALGORITHM WITH CHILDREN MANIPULATION
		If its full, sort it and make two new child nodes, Leaf Splitting with children Algorithm:
	        1. Pick middle element by using length of array/2, lets say its index i (Same as 3.4.1)
			2. Club all elements from 0 to i-1, and i+1 to len(lkeys array) and create new seperate nodes
			   by inserting these 2 arrays into the respective keys[] of respective nodes (Same as 3.4.2)
			3. For children[], split the current node's children array into 2 parts, part1 will be
			   from 0 to i, and part 2 will be from i+1 to len(children array), and insert them into
			   leftNode children, and rightNode children

		NOTE : NODE CREATION WILL TAKE PLACE HERE
	*/
	elements := n.getElements()
	midIndex := len(elements) / 2
	middle := elements[midIndex]

	// Now lets split elements array into 2 as we are splitting this node
	elements1 := elements[0:midIndex]
	elements2 := elements[midIndex+1:]

	// Lets split the children
	children := n.childrenBlockIDs

	children1 := children[0 : midIndex+1]
	children2 := children[midIndex+1:]

	// Now lets construct new Nodes from these 2 element arrays
	leftNode, err := newNodeWithChildren(elements1, children1, n.blockService)
	if err != nil {
		return nil, nil, nil, err
	}
	rightNode, err := newNodeWithChildren(elements2, children2, n.blockService)
	if err != nil {
		return nil, nil, nil, err
	}
	return middle, leftNode, rightNode, nil
}

// addPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren inserts a popped-up element into the current node.
func (n *DiskNode) addPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(element *pairs, leftNode *DiskNode, rightNode *DiskNode) {
	/**
		POPPED UP JOINING ALGORITHM
			Insert into current Node, Popped up element and two child pointers insertion algorithm, Popped Up Joining Algorithm:
	        1. Insert element and sort the array
	        2. Now we need to discard 1 child pointer and insert 2 child pointers, Child Pointer Manipulation Algorithm :
	        3. Find index of inserted element in array, lets say that it is i
	        4. Now in the child pointer array, insert the left and right pointers at ith and i+1 th index
	*/

	//CHILD POINTER MANIPULATION ALGORITHM
	insertionIndex := n.addElement(element)
	n.setChildAtIndex(insertionIndex, leftNode)
	//Shift remaining elements to the right and add this
	n.shiftRemainingChildrenToRight(insertionIndex + 1)
	n.setChildAtIndex(insertionIndex+1, rightNode)
}

// newLeafNode creates a new leaf node without children.
func newLeafNode(elements []*pairs, bs *blockService) (*DiskNode, error) {
	node := &DiskNode{keys: elements, blockService: bs}
	//persist the node to disk
	err := bs.saveNewNodeToDisk(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// newNodeWithChildren creates a new non-leaf node with children.
func newNodeWithChildren(elements []*pairs, childrenBlockIDs []uint64, bs *blockService) (*DiskNode, error) {
	node := &DiskNode{keys: elements, childrenBlockIDs: childrenBlockIDs, blockService: bs}
	//persist this node to disk
	err := bs.saveNewNodeToDisk(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// newRootNodeWithSingleElementAndTwoChildren creates a new root node with a single element and two children.
func newRootNodeWithSingleElementAndTwoChildren(element *pairs, leftChildBlockID uint64,
	rightChildBlockID uint64, bs *blockService) (*DiskNode, error) {
	elements := []*pairs{element}
	childrenBlockIDs := []uint64{leftChildBlockID, rightChildBlockID}
	node := &DiskNode{keys: elements, childrenBlockIDs: childrenBlockIDs, blockService: bs}
	//persist this node to disk
	err := bs.updateRootNode(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// getChildNodeForElement - Get Correct Traversal path for insertion
func (n *DiskNode) getChildNodeForElement(key string) (*DiskNode, error) {
	/** CHILD NODE SEARCHING ALGORITHM
		If this is not a leaf node, then find out the proper child node, Child Node Searching Algorithm:
	    1. Input : Value to be inserted, the current Node. Output : Pointer to the childnode
		2. Since the list of values/elements is sorted, perform a binary or linear search to find the
		   first element greater than the value to be inserted, if such an element is found, return pointer at position i, else return last pointer ( ie. the last pointer)
	*/

	for i := 0; i < len(n.getElements()); i++ {
		if key < n.getElementAtIndex(i).key {
			return n.getChildAtIndex(i)
		}
	}
	// This means that no element is found with value greater than the element to be inserted
	// so we need to return the last child node
	return n.getLastChildNode()
}

// insert inserts a key-value pair into the B-tree.
func (n *DiskNode) insert(value *pairs, bt *btree) (*pairs, *DiskNode, *DiskNode, error) {
	if n.isLeaf() {
		n.addElement(value)
		if !n.hasOverFlown() {
			// So lets store this updated node on disk
			err := n.blockService.updateNodeToDisk(n)
			if err != nil {
				return nil, nil, nil, err
			}
			return nil, nil, nil, nil
		}
		if bt.isRootNode(n) {
			poppedMiddleElement, leftNode, rightNode, err := n.splitLeafNode()
			if err != nil {
				return nil, nil, nil, err
			}
			//NOTE : NODE CREATION WILL TAKE PLACE HERE
			newRootNode, err := newRootNodeWithSingleElementAndTwoChildren(poppedMiddleElement,
				leftNode.blockID, rightNode.blockID, n.blockService)
			if err != nil {
				return nil, nil, nil, err
			}
			bt.setRootNode(newRootNode)
			return nil, nil, nil, nil

		}
		// Split the node and return to parent function with pooped up element and left,right nodes
		return n.splitLeafNode()

	}
	// Get the child Node for insertion
	childNodeToBeInserted, err := n.getChildNodeForElement(value.key)
	if err != nil {
		return nil, nil, nil, err
	}
	poppedMiddleElement, leftNode, rightNode, err := childNodeToBeInserted.insert(value, bt)
	if err != nil {
		return nil, nil, nil, err
	}
	if poppedMiddleElement == nil {
		// this means element has been inserted into the child and hence we do nothing
		return poppedMiddleElement, leftNode, rightNode, nil
	}
	// Insert popped up element into current node along with updating the child pointers
	// with new left and right nodes returned
	n.addPoppedUpElementIntoCurrentNodeAndUpdateWithNewChildren(poppedMiddleElement, leftNode, rightNode)

	if !n.hasOverFlown() {
		// this means that element has been easily inserted into current parent Node
		// without overflowing
		err := n.blockService.updateNodeToDisk(n)
		if err != nil {
			return nil, nil, nil, err
		}
		// So lets store this updated node on disk
		return nil, nil, nil, nil
	}
	// this means that the current parent node has overflown, we need to split this up
	// and move the popped up element upwards if this is not the root
	poppedMiddleElement, leftNode, rightNode, err = n.splitNonLeafNode()
	if err != nil {
		return nil, nil, nil, err
	}
	/**
		If current node is not the root node return middle,leftNode,rightNode
	    else if current node == rootNode, Root Node Splitting Algorithm:
	            1. Create a new node with elements array as keys[0] = middle
	            2. children[0]=leftNode and children[1]=rightNode
	            3. Set btree.root=new node
	            4. return null,null,null
	*/

	if !bt.isRootNode(n) {
		return poppedMiddleElement, leftNode, rightNode, nil
	}
	newRootNode, err := newRootNodeWithSingleElementAndTwoChildren(poppedMiddleElement,
		leftNode.blockID, rightNode.blockID, n.blockService)
	if err != nil {
		return nil, nil, nil, err
	}

	//@Todo: Update the metadata somewhere so that we can read this new root node
	//next time
	bt.setRootNode(newRootNode)
	return nil, nil, nil, nil
}

// searchElementInNode searches for an element within the current node.
func (n *DiskNode) searchElementInNode(key string) (string, bool) {
	elements := n.getElements()
	low, high := 0, len(elements)-1

	for low <= high {
		middle := low + (high-low)/2
		middleKey := elements[middle].key

		if middleKey == key {
			return elements[middle].value, true
		} else if middleKey < key {
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	return "", false
}

// search searches for a key in the B-tree.
func (n *DiskNode) search(key string) (string, error) {
	/*
		Algo:
		1. Find key in current node, if this is leaf node, then return as not found
		2. Then find the appropriate child node
		3. goto step 1
	*/
	value, foundInCurrentNode := n.searchElementInNode(key)

	if foundInCurrentNode {
		return value, nil
	}

	if n.isLeaf() {
		return "", nil
	}

	node, err := n.getChildNodeForElement(key)
	if err != nil {
		return "", err
	}
	return node.search(key)
}

// InsertPair inserts a key-value pair into the B-tree.
func (n *DiskNode) insertPair(value *pairs, bt *btree) error {
	_, _, _, err := n.insert(value, bt)
	if err != nil {
		return err
	}
	return nil
}

// DeletePair deletes a key-value pair from the B-tree.
func (n *DiskNode) deletePair(value *pairs, bt *btree) error {
	return n.delete(value.key, bt)
}

// delete deletes a key from the B-tree.
func (n *DiskNode) delete(key string, bt *btree) error {
	// First locate the leaf node containing the key
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.isLeaf() {
		childNode, err := n.getChildNodeForElement(key)
		if err != nil {
			return err
		}
		return childNode.delete(key, bt)
	}

	// We are at leaf node, try to delete the key
	elements := n.getElements()
	indexToDelete := -1
	for i, element := range elements {
		if element.key == key {
			indexToDelete = i
			break
		}
	}

	if indexToDelete == -1 {
		return fmt.Errorf("key %s not found", key)
	}

	// Remove the element
	n.setElements(append(elements[:indexToDelete], elements[indexToDelete+1:]...))

	// Check if node has underflown
	minSize := (n.blockService.getMaxLeafSize() + 1) / 2
	if len(n.getElements()) >= minSize || bt.isRootNode(n) {
		// Node has enough elements or is root, just update it
		return n.blockService.updateNodeToDisk(n)
	}

	// Handle underflow by either borrowing or merging
	return n.handleUnderflow(bt)
}

// findParentInfo returns the parent node, index of the current node in parent's children,
// and any error that occurred
type parentInfo struct {
	// parent is the parent node.
	parent *DiskNode
	// index is the index of the current node in the parent's children.
	index int
}

// findParentInfo returns the parent node information for the current node.
func (n *DiskNode) findParentInfo(current node) (*parentInfo, error) {
	// Type assert to check if current node is a DiskNode
	diskNode, ok := current.(*DiskNode)
	if !ok {
		return nil, fmt.Errorf("expected DiskNode, got %T", current)
	}

	if diskNode.isLeaf() {
		return nil, nil
	}

	// Get all child nodes
	childNodes, err := diskNode.getChildNodes()
	if err != nil {
		return nil, err
	}

	// Check each child
	for i, child := range childNodes {
		if child.blockID == n.blockID {
			return &parentInfo{parent: diskNode, index: i}, nil
		}

		// Recursively search in child
		if info, err := n.findParentInfo(child); err != nil {
			return nil, err
		} else if info != nil {
			return info, nil
		}
	}

	return nil, nil
}

// handleUnderflow handles node underflow by borrowing or merging with sibling nodes.
func (n *DiskNode) handleUnderflow(bt *btree) error {
	// Find parent information
	parentInfo, err := n.findParentInfo(bt.root)
	if err != nil {
		return err
	}
	if parentInfo == nil {
		return fmt.Errorf("parent not found for node")
	}

	parent := parentInfo.parent
	parentIndex := parentInfo.index

	// Try to borrow from left sibling
	if parentIndex > 0 {
		leftSibling, err := parent.getChildAtIndex(parentIndex - 1)
		if err != nil {
			return err
		}

		if len(leftSibling.getElements()) > (n.blockService.getMaxLeafSize()+1)/2 {
			return n.borrowFromLeft(leftSibling, parent, parentIndex)
		}
	}

	// Try to borrow from right sibling
	if parentIndex < len(parent.childrenBlockIDs)-1 {
		rightSibling, err := parent.getChildAtIndex(parentIndex + 1)
		if err != nil {
			return err
		}

		if len(rightSibling.getElements()) > (n.blockService.getMaxLeafSize()+1)/2 {
			return n.borrowFromRight(rightSibling, parent, parentIndex)
		}
	}

	// If we can't borrow, we need to merge
	// Prefer merging with left sibling if possible
	if parentIndex > 0 {
		leftSibling, err := parent.getChildAtIndex(parentIndex - 1)
		if err != nil {
			return err
		}
		return n.mergeWithLeft(leftSibling, parent, parentIndex, bt)
	}

	// Otherwise merge with right sibling
	rightSibling, err := parent.getChildAtIndex(parentIndex + 1)
	if err != nil {
		return err
	}
	return n.mergeWithRight(rightSibling, parent, parentIndex, bt)
}

// borrowFromLeft borrows an element from the left sibling node.
func (n *DiskNode) borrowFromLeft(leftSibling *DiskNode, parent *DiskNode, parentIndex int) error {
	// Move the rightmost element from left sibling to current node
	elements := leftSibling.getElements()
	borrowedElement := elements[len(elements)-1]
	leftSibling.setElements(elements[:len(elements)-1])

	// Add borrowed element to current node
	n.setElements(append([]*pairs{borrowedElement}, n.getElements()...))

	// Update parent's key
	parent.keys[parentIndex-1] = n.getElements()[0]

	// Save changes to disk
	if err := leftSibling.blockService.updateNodeToDisk(leftSibling); err != nil {
		return err
	}
	if err := n.blockService.updateNodeToDisk(n); err != nil {
		return err
	}
	return parent.blockService.updateNodeToDisk(parent)
}

// borrowFromRight borrows an element from the right sibling node.
func (n *DiskNode) borrowFromRight(rightSibling *DiskNode, parent *DiskNode, parentIndex int) error {
	// Move the leftmost element from right sibling to current node
	elements := rightSibling.getElements()
	borrowedElement := elements[0]
	rightSibling.setElements(elements[1:])

	// Add borrowed element to current node
	n.setElements(append(n.getElements(), borrowedElement))

	// Update parent's key
	parent.keys[parentIndex] = rightSibling.getElements()[0]

	// Save changes to disk
	if err := rightSibling.blockService.updateNodeToDisk(rightSibling); err != nil {
		return err
	}
	if err := n.blockService.updateNodeToDisk(n); err != nil {
		return err
	}
	return parent.blockService.updateNodeToDisk(parent)
}

// mergeWithLeft merges the current node with the left sibling node.
func (n *DiskNode) mergeWithLeft(leftSibling *DiskNode, parent *DiskNode, parentIndex int, bt *btree) error {
	// Merge current node's elements into left sibling
	leftSibling.setElements(append(leftSibling.getElements(), n.getElements()...))

	// Update parent's children
	parent.childrenBlockIDs = append(parent.childrenBlockIDs[:parentIndex], parent.childrenBlockIDs[parentIndex+1:]...)
	parent.keys = append(parent.keys[:parentIndex-1], parent.keys[parentIndex:]...)

	// Handle parent underflow if needed
	if len(parent.keys) < (parent.blockService.getMaxLeafSize()+1)/2 && !bt.isRootNode(parent) {
		return parent.handleUnderflow(bt)
	}

	// Save changes to disk
	if err := leftSibling.blockService.updateNodeToDisk(leftSibling); err != nil {
		return err
	}
	return parent.blockService.updateNodeToDisk(parent)
}

// mergeWithRight merges the current node with the right sibling node.
func (n *DiskNode) mergeWithRight(rightSibling *DiskNode, parent *DiskNode, parentIndex int, bt *btree) error {
	// Merge right sibling's elements into current node
	n.setElements(append(n.getElements(), rightSibling.getElements()...))

	// Update parent's children
	parent.childrenBlockIDs = append(parent.childrenBlockIDs[:parentIndex+1], parent.childrenBlockIDs[parentIndex+2:]...)
	parent.keys = append(parent.keys[:parentIndex], parent.keys[parentIndex+1:]...)

	// Handle parent underflow if needed
	if len(parent.keys) < (parent.blockService.getMaxLeafSize()+1)/2 && !bt.isRootNode(parent) {
		return parent.handleUnderflow(bt)
	}

	// Save changes to disk
	if err := n.blockService.updateNodeToDisk(n); err != nil {
		return err
	}
	return parent.blockService.updateNodeToDisk(parent)
}

// getValue returns the value associated with the given key.
func (n *DiskNode) getValue(key string) (string, error) {
	return n.search(key)
}
