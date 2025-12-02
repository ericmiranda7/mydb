package util

type TreeMap struct {
	root *TreeNode
	size int
}

type TreeNode struct {
	key   string
	value string
	left  *TreeNode
	right *TreeNode
}

type Entry struct {
	Key   string
	Value string
}

func NewTreeMap() *TreeMap {
	return &TreeMap{root: nil}
}

func (tm *TreeMap) Insert(key, value string) {
	tm.root = insert(tm.root, key, value)
	tm.size += len(key) + len(value)
}

func insert(root *TreeNode, key, value string) *TreeNode {
	if root == nil {
		return &TreeNode{
			key:   key,
			value: value,
		}
	}

	if key < root.key {
		root.left = insert(root.left, key, value)
	} else if key > root.key {
		root.right = insert(root.right, key, value)
	} else {
		root.value = value
	}
	return root
}

func (tm *TreeMap) Get(key string) (string, bool) {
	return get(tm.root, key)
}

func get(root *TreeNode, key string) (string, bool) {
	if root == nil {
		return "", false
	}
	if root.key == key {
		return root.value, true
	} else if key < root.key {
		return get(root.left, key)
	} else {
		return get(root.right, key)
	}
}

func (tm *TreeMap) GetInorder() []Entry {
	var res []Entry
	inorder(tm.root, &res)
	return res
}

func inorder(root *TreeNode, res *[]Entry) {
	if root == nil {
		return
	}

	inorder(root.left, res)
	*res = append(*res, Entry{Key: root.key, Value: root.value})
	inorder(root.right, res)
}

func (tm *TreeMap) GetSize() int {
	return tm.size
}
