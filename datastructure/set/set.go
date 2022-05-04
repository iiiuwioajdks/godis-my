package set

import "qydis/datastructure/dict"

/*
set use hash table
 */

type Set struct {
	dict dict.Dict
}

// use a simple hash table , dict.map
