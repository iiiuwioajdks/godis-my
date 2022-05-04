package sortedset

import "math/rand"

/*
跳表(skiplist) 是 Redis 中 SortedSet 数据结构的底层实现, 跳表优秀的范围查找能力为ZRange和ZRangeByScore等命令提供了支持。
 */

type Element struct {
	Member string
	Score float64
}

// Node 每一个节点都可能在多层， level就是包括了该节点在哪一层
type Node struct {
	Element
	// 指向前一个节点
	backward *Node
	// level[0]表示最底层
	level []*Level
}

// Level 是每一层的所有节点
type Level struct {
	forward *Node // 指向下一个节点
	span int64 // 到forward跳过的节点数
}

type skiplist struct {
	header *Node
	tail *Node
	length int64
	level int16
}

func makeNode(level int16, ele *Element) *Node {
	node := &Node{
		level: make([]*Level, level),
		Element: Element{
			Member: ele.Member,
			Score: ele.Score,
		},
	}
	for i := range node.level {
		node.level[i] =new(Level)
	}
	return node
}

const maxLevel = 16
// 插入节点
func (s *skiplist) insert(ele *Element) *Node {
	// 寻找新节点的先驱节点，它们的 forward 将指向新节点
	// 因为每层都有一个 forward 指针, 所以每层都会对应一个先驱节点
	// 找到这些先驱节点并保存在 update 数组中
	update := make([]*Node, maxLevel)
	rank := make([]int64, maxLevel) // 保存各层先驱节点的排名，用于计算span

	// 从上层往下寻找
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		// 初始化最上层的rank
		if i == s.level - 1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < ele.Score ||
					node.level[i].forward.Score == ele.Score && node.level[i].forward.Member < ele.Member) {
				// 每寻找一次就说明跳过了 span 个节点
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	// 如果插入一个节点就需要插入所有的层，那开销就太大了
	// 所以以随机层数来决定，第一层可能性为1，第二层可能性为1/4...
	level := randomLevel()
	// level 可能比现在有的层次还要高
	if level > s.level {
		for i := s.level; i < level; i++ {
			rank[i] = 0
			update[i] = s.header
			update[i].level[i].span = s.length
		}
		s.level = level
	}
	// 插入新节点
	node = makeNode(level, ele)
	for i := int16(0); i < level; i++ {
		// 新节点的forward指向先驱节点的 forward(后一个节点
		node.level[i].forward = update[i].level[i].forward
		// 先驱节点的forward指向新节点
		update[i].level[i].forward = node

		// 计算先驱节点和新节点的span
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 新节点可能不会包含所有层
	// 对于没有层，先驱节点的 span 会加1 (后面插入了新节点导致span+1)
	for i := level; i < s.level; i++ {
		update[i].level[i].span++
	}

	// 更新后向指针(前驱)
	if update[0] == s.header {
		node.backward = nil
	} else  {
		node.backward = update[0]
	}
	// 让下一个节点的前驱指向新节点
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		s.tail = node
	}
	s.length++
	return node
}

// 随机结果出现2的概率是出现1的25%，出现3的概率是出现2的25%
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}

func (s *skiplist) getByRank(rank int64) *Node {
	var index int64 = 0
	n := s.header
	// 从上往下寻找
	for level := s.level -1; level > 0; level-- {
		// 从当前层向前搜索
		// 若当前层的下一个节点已经超过目标 (i+n.level[level].span > rank)，则结束当前层搜索进入下一层
		for n.level[level].forward != nil && (index+n.level[level].span) <= rank {
			index += n.level[level].span
			n = n.level[level].forward
		}
		if rank == index {
			return n
		}
	}
	return nil
}

// 获得第一个在 min-max 范围内的节点
func (s *skiplist) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *Node {
	// 判断跳表和范围是否有交集，若无交集提早返回
	if !s.hasInRange(min, max) {
		return nil
	}
	n := s.header
	// 自顶向下查找
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	// 当从外层循环退出时 level=0 (最下层), n.level[0].forward 一定是 min 范围内的第一个节点
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

func (skiplist *skiplist) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	// min & max = empty
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	// min > tail
	n := skiplist.tail
	if n == nil || !min.less(n.Score) {
		return false
	}
	// max < head
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}
	return true
}

// RemoveRangeByRank 删除操作可能一次删除多个节点
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64)(removed []*Element) {
	var i int64 = 0
	update := make([]*Node, maxLevel)
	removed = make([]*Element,0)

	// 自顶向下查找
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i + node.level[level].span < start) {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}
	i++
	node = node.level[0].forward

	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}

// 传入目标节点和删除后的先驱节点
// 在批量删除时我们传入的 update 数组是相同的
func (skiplist *skiplist) removeNode(node *Node, update []*Node) {
	for i := int16(0); i < skiplist.level; i++ {
		// 如果先驱节点的forward指针指向了目标节点，则需要修改先驱的forward指针跳过要删除的目标节点
		// 同时更新先驱的 span
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			// 说明待删除的节点没有在这一层，但是下面的层会少一个节点，所以span还是需要减一
			update[i].level[i].span--
		}
	}
	// 修改目标节点后继节点的backward指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		// 说明是 跳表的最后一个元素，就要更新tail
		skiplist.tail = node.backward
	}
	// 必要时删除空白的层
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}