// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/rangedel"
)

type mergingIterLevel struct {
	iter internalIterator
	// rangeDelIter is set to the range-deletion iterator for the level. When
	// configured with a levelIter, this pointer changes as sstable boundaries
	// are crossed. See levelIter.initRangeDel and the Range Deletions comment
	// below.
	rangeDelIter internalIterator
	// iterKey and iterValue cache the current key and value iter are pointed at.
	iterKey   *InternalKey
	iterValue []byte

	// smallestUserKey, largestUserKey, isLargestUserKeyRangeDelSentinel are set using the sstable
	// boundary keys when using levelIter. See levelIter comment and the Range Deletions comment
	// below.
	smallestUserKey, largestUserKey  []byte
	isLargestUserKeyRangeDelSentinel bool

	// tombstone caches the tombstone rangeDelIter is currently pointed at. If
	// tombstone.Empty() is true, there are no further tombstones within the
	// current sstable in the current iterator direction. The cached tombstone is
	// only valid for the levels in the range [0,heap[0].index]. This avoids
	// positioning tombstones at lower levels which cannot possibly shadow the
	// current key.
	tombstone rangedel.Tombstone
}

// mergingIter provides a merged view of multiple iterators from different
// levels of the LSM.
//
// The core of a mergingIter is a heap of internalIterators (see
// mergingIterHeap). The heap can operate as either a min-heap, used during
// forward iteration (First, SeekGE, Next) or a max-heap, used during reverse
// iteration (Last, SeekLT, Prev). The heap is initialized in calls to First,
// Last, SeekGE, and SeekLT. A call to Next or Prev takes the current top
// element on the heap, advances its iterator, and then "fixes" the heap
// property. When one of the child iterators is exhausted during Next/Prev
// iteration, it is removed from the heap.
//
// Range Deletions
//
// A mergingIter can optionally be configured with a slice of range deletion
// iterators. The range deletion iterator slice must exactly parallel the point
// iterators and the range deletion iterator must correspond to the same level
// in the LSM as the point iterator. Note that each memtable and each table in
// L0 is a different "level" from the mergingIter perspective. So level 0 below
// does not correspond to L0 in the LSM.
//
// A range deletion iterator iterates over fragmented range tombstones. Range
// tombstones are fragmented by splitting them at any overlapping points. This
// fragmentation guarantees that within an sstable tombstones will either be
// distinct or will have identical start and end user keys. While range
// tombstones are fragmented within an sstable, the start and end keys are not truncated
// to sstable boundaries. This is necessary because the tombstone end key is
// exclusive and does not have a sequence number. Consider an sstable
// containing the range tombstone [a,c)#9 and the key "b#8". The tombstone must
// delete "b#8", yet older versions of "b" might spill over to the next
// sstable. So the boundary key for this sstable must be "b#8". Adjusting the
// end key of tombstones to be optionally inclusive or contain a sequence
// number would be possible solutions (such solutions have potentially serious
// issues: tombstones have exclusive end keys since an inclusive deletion end can
// be converted to an exclusive one while the reverse transformation is not possible;
// the semantics of a sequence number for the end key of a range tombstone are murky).
//
// The approach taken here performs an
// implicit truncation of the tombstone to the sstable boundaries.
//
// During initialization of a mergingIter, the range deletion iterators for
// batches, memtables, and L0 tables are populated up front. Note that Batches
// and memtables index unfragmented tombstones.  Batch.newRangeDelIter() and
// memTable.newRangeDelIter() fragment and cache the tombstones on demand. The
// L1-L6 range deletion iterators are populated by levelIter. When configured
// to load range deletion iterators, whenever a levelIter loads a table it
// loads both the point iterator and the range deletion
// iterator. levelIter.rangeDelIter is configured to point to the right entry
// in mergingIter.levels. The effect of this setup is that
// mergingIter.levels[i].rangeDelIter always contains the fragmented range
// tombstone for the current table in level i that the levelIter has open.
//
// Another crucial mechanism of levelIter is that it materializes fake point
// entries for the table boundaries if the boundary is range deletion
// key. Consider a table that contains only a range tombstone [a-e)#10. The
// sstable boundaries for this table will be a#10,15 and
// e#72057594037927935,15. During forward iteration levelIter will return
// e#72057594037927935,15 as a key. During reverse iteration levelIter will
// return a#10,15 as a key. These sentinel keys act as bookends to point
// iteration and allow mergingIter to keep a table and its associated range
// tombstones loaded as long as there are keys at lower levels that are within
// the bounds of the table.
//
// The final piece to the range deletion puzzle is the LSM invariant that for a
// given key K newer versions of K can only exist earlier in the level, or at
// higher levels of the tree. For example, if K#4 exists in L3, k#5 can only
// exist earlier in the L3 or in L0, L1, L2 or a memtable. Get very explicitly
// uses this invariant to find the value for a key by walking the LSM level by
// level. For range deletions, this invariant means that a range deletion at
// level N will necessarily shadow any keys within its bounds in level Y where
// Y > N. One wrinkle to this statement is that it only applies to keys that
// lie within the sstable bounds as well, but we get that guarantee due to the
// way the range deletion iterator and point iterator are bound together by a
// levelIter.
//
// Tying the above all together, we get a picture where each level (index in
// mergingIter.levels) is composed of both point operations (pX) and range
// deletions (rX). The range deletions for level X shadow both the point
// operations and range deletions for level Y where Y > X allowing mergingIter
// to skip processing entries in that shadow. For example, consider the
// scenario:
//
//   r0: a---e
//   r1:    d---h
//   r2:       g---k
//   r3:          j---n
//   r4:             m---q
//
// This is showing 5 levels of range deletions. Consider what happens upon
// SeekGE("b"). We first seek the point iterator for level 0 (the point values
// are not shown above) and we then seek the range deletion iterator. That
// returns the tombstone [a,e). This tombstone tells us that all keys in the
// range [a,e) in lower levels are deleted so we can skip them. So we can
// adjust the seek key to "e", the tombstone end key. For level 1 we seek to
// "e" and find the range tombstone [d,h) and similar logic holds. By the time
// we get to level 4 we're seeking to "n".
//
// One consequence of not truncating tombstone end keys to sstable boundaries
// is the seeking process described above cannot always seek to the tombstone
// end key in the older level. For example, imagine in the above example r3 is
// a partitioned level (i.e., L1+ in our LSM), and the sstable containing [j,
// n) has "k" as its upper boundary. In this situation, compactions involving
// keys at or after "k" can output those keys to r4+, even if they're newer
// than our tombstone [j, n). So instead of seeking to "n" in r4 we can only
// seek to "k".  To achieve this, the instance variable `largestUserKey.`
// maintains the upper bounds of the current sstables in the partitioned
// levels. In this example, `levels[3].largestUserKey` holds "k", telling us to
// limit the seek triggered by a tombstone in r3 to "k".
//
// During actual iteration levels can contain both point operations and range
// deletions. Within a level, when a range deletion contains a point operation
// the sequence numbers must be checked to determine if the point operation is
// newer or older than the range deletion tombstone. The mergingIter maintains
// the invariant that the range deletion iterators for all levels newer that
// the current iteration key (L < m.heap.items[0].index) are positioned at the
// next (or previous during reverse iteration) range deletion tombstone. We
// know those levels don't contain a range deletion tombstone that covers the
// current key because if they did the current key would be deleted. The range
// deletion iterator for the current key's level is positioned at a range
// tombstone covering or past the current key. The position of all of other
// range deletion iterators is unspecified. Whenever a key from those levels
// becomes the current key, their range deletion iterators need to be
// positioned. This lazy positioning avoids seeking the range deletion
// iterators for keys that are never considered. (A similar bit of lazy
// evaluation can be done for the point iterators, but is still TBD).
//
// For a full example, consider the following setup:
//
//   p0:               o
//   r0:             m---q
//
//   p1:              n p
//   r1:       g---k
//
//   p2:  b d    i
//   r2: a---e           q----v
//
//   p3:     e
//   r3:
//
// If we start iterating from the beginning, the first key we encounter is "b"
// in p2. When the mergingIter is pointing at a valid entry, the range deletion
// iterators for all of the levels < m.heap.items[0].index are positioned at
// the next range tombstone past the current key. So r0 will point at [m,q) and
// r1 at [g,k). When the key "b" is encountered, we check to see if the current
// tombstone for r0 or r1 contains it, and whether the tombstone for r2, [a,e),
// contains and is newer than "b".
//
// Advancing the iterator finds the next key at "d". This is in the same level
// as the previous key "b" so we don't have to reposition any of the range
// deletion iterators, but merely check whether "d" is now contained by any of
// the range tombstones at higher levels or has stepped past the range
// tombstone in its own level or higher levels. In this case, there is nothing to be done.
//
// Advancing the iterator again finds "e". Since "e" comes from p3, we have to
// position the r3 range deletion iterator, which is empty. "e" is past the r2
// tombstone of [a,e) so we need to advance the r2 range deletion iterator to
// [q,v).
//
// The next key is "i". Because this key is in p2, a level above "e", we don't
// have to reposition any range deletion iterators and instead see that "i" is
// covered by the range tombstone [g,k). The iterator is immediately advanced
// to "n" which is covered by the range tombstone [m,q) causing the iterator to
// advance to "o" which is visible.
//
// TODO(peter,rangedel): For testing, advance the iterator through various
// scenarios and have each step display the current state (i.e. the current
// heap and range-del iterator positioning).
type mergingIter struct {
	logger   Logger
	dir      int
	snapshot uint64
	levels   []mergingIterLevel
	heap     mergingIterHeap
	err      error
	prefix   []byte
	lower    []byte
	upper    []byte

	// Elide range tombstones from being returned during iteration. Set to true
	// when mergingIter is a child of Iterator and the mergingIter is processing
	// range tombstones.
	elideRangeTombstones bool
}

// mergingIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*mergingIter)(nil)

// newMergingIter returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
//
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
//
// None of the iters may be nil.
func newMergingIter(logger Logger, cmp Compare, iters ...internalIterator) *mergingIter {
	m := &mergingIter{}
	levels := make([]mergingIterLevel, len(iters))
	for i := range levels {
		levels[i].iter = iters[i]
	}
	m.init(&IterOptions{logger: logger}, cmp, levels...)
	return m
}

func (m *mergingIter) init(opts *IterOptions, cmp Compare, levels ...mergingIterLevel) {
	m.err = nil
	m.logger = opts.getLogger()
	if opts != nil {
		m.lower = opts.LowerBound
		m.upper = opts.UpperBound
	}
	m.snapshot = InternalKeySeqNumMax
	m.levels = levels
	m.heap.cmp = cmp
	m.heap.items = make([]mergingIterItem, 0, len(levels))
}

func (m *mergingIter) initHeap() {
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		if l := &m.levels[i]; l.iterKey != nil {
			m.heap.items = append(m.heap.items, mergingIterItem{
				index: i,
				key:   *l.iterKey,
				value: l.iterValue,
			})
		} else {
			m.err = firstError(m.err, l.iter.Error())
			if m.err != nil {
				return
			}
		}
	}
	m.heap.init()
}

func (m *mergingIter) initMinHeap() {
	m.dir = 1
	m.heap.reverse = false
	m.initHeap()
	m.initMinRangeDelIters(-1)
}

// The level of the previous top element was oldTopLevel. Note that all range delete
// iterators < oldTopLevel are positioned past the key of the previous top element and
// the range delete iterator == oldTopLevel is positioned at or past the key of the
// previous top element. We need to position the range delete iterators from oldTopLevel + 1
// to the level of the current top element.
func (m *mergingIter) initMinRangeDelIters(oldTopLevel int) {
	if m.heap.len() == 0 {
		return
	}

	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := &m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil {
			continue
		}
		l.tombstone = rangedel.SeekGE(m.heap.cmp, l.rangeDelIter, item.key.UserKey, m.snapshot)
	}
}

func (m *mergingIter) initMaxHeap() {
	m.dir = -1
	m.heap.reverse = true
	m.initHeap()
	m.initMaxRangeDelIters(-1)
}

// The level of the previous top element was oldTopLevel. Note that all range delete
// iterators < oldTopLevel are positioned before the key of the previous top element and
// the range delete iterator == oldTopLevel is positioned at or before the key of the
// previous top element. We need to position the range delete iterators from oldTopLevel + 1
// to the level of the current top element.
func (m *mergingIter) initMaxRangeDelIters(oldTopLevel int) {
	if m.heap.len() == 0 {
		return
	}
	// Position the range-del iterators at levels <= m.heap.items[0].index.
	item := &m.heap.items[0]
	for level := oldTopLevel + 1; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil {
			continue
		}
		l.tombstone = rangedel.SeekLE(m.heap.cmp, l.rangeDelIter, item.key.UserKey, m.snapshot)
	}
}

func (m *mergingIter) switchToMinHeap() {
	if m.heap.len() == 0 {
		if m.lower != nil {
			m.SeekGE(m.lower)
		} else {
			m.First()
		}
		return
	}

	// We're switching from using a max heap to a min heap. We need to advance
	// any iterator that is less than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
	//
	// i1:     *a:2     b:2
	// i2: a:1      b:1
	//
	// The current key is a:2 and i2 is pointed at a:1. When we switch to forward
	// iteration, we want to return a key that is greater than a:2.

	key := m.heap.items[0].key
	cur := &m.levels[m.heap.items[0].index]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}
		if l.iterKey == nil {
			if m.lower != nil {
				l.iterKey, l.iterValue = l.iter.SeekGE(m.lower)
			} else {
				l.iterKey, l.iterValue = l.iter.First()
			}
		}
		for ; l.iterKey != nil; l.iterKey, l.iterValue = l.iter.Next() {
			if base.InternalCompare(m.heap.cmp, key, *l.iterKey) < 0 {
				// key < iter-key
				break
			}
			// key >= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.iterKey, cur.iterValue = cur.iter.Next()
	m.initMinHeap()
}

func (m *mergingIter) switchToMaxHeap() {
	if m.heap.len() == 0 {
		if m.upper != nil {
			m.SeekLT(m.upper)
		} else {
			m.Last()
		}
		return
	}

	// We're switching from using a min heap to a max heap. We need to backup any
	// iterator that is greater than or equal to the current key. Consider the
	// scenario where we have 2 iterators being merged (user-key:seq-num):
	//
	// i1: a:2     *b:2
	// i2:     a:1      b:1
	//
	// The current key is b:2 and i2 is pointing at b:1. When we switch to
	// reverse iteration, we want to return a key that is less than b:2.
	key := m.heap.items[0].key
	cur := &m.levels[m.heap.items[0].index]

	for i := range m.levels {
		l := &m.levels[i]
		if l == cur {
			continue
		}
		if l.iterKey == nil {
			if m.upper != nil {
				l.iterKey, l.iterValue = l.iter.SeekLT(m.upper)
			} else {
				l.iterKey, l.iterValue = l.iter.Last()
			}
		}
		for ; l.iterKey != nil; l.iterKey, l.iterValue = l.iter.Prev() {
			if base.InternalCompare(m.heap.cmp, key, *l.iterKey) > 0 {
				// key > iter-key
				break
			}
			// key <= iter-key
		}
	}

	// Special handling for the current iterator because we were using its key
	// above.
	cur.iterKey, cur.iterValue = cur.iter.Prev()
	m.initMaxHeap()
}

// Steps to the next entry. item is the current top item in the heap.
func (m *mergingIter) nextEntry(item *mergingIterItem) {
	l := &m.levels[item.index]
	oldTopLevel := item.index
	oldRangeDelIter := l.rangeDelIter
	if l.iterKey, l.iterValue = l.iter.Next(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
		if l.rangeDelIter != oldRangeDelIter {
			// The rangeDelIter changed which indicates that the l.iter moved to the
			// next sstable. We have to update the tombstone for oldTopLevel as well.
			oldTopLevel--
		}
	} else {
		m.err = l.iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}

	// The cached tombstones are only valid for the levels
	// [0,oldTopLevel]. Updated the cached tombstones for any levels in the range
	// [oldTopLevel+1,heap[0].index].
	m.initMinRangeDelIters(oldTopLevel)
}

// isNextEntryDeleted() starts from the current entry (as the next entry) and if it is deleted,
// moves the iterators forward as needed and returns true, else it returns false. item is the top
// item in the heap.
func (m *mergingIter) isNextEntryDeleted(item *mergingIterItem) bool {
	// Look for a range deletion tombstone containing item.key at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil || l.tombstone.Empty() {
			// If l.tombstone.Empty() is true, there are no further tombstones in the
			// current sstable in the current (forward) iteration direction.
			continue
		}
		if m.heap.cmp(l.tombstone.End, item.key.UserKey) <= 0 {
			// The current key is at or past the tombstone end key.
			//
			// NB: for the case that this l.rangeDelIter is provided by a levelIter we know that
			// the levelIter must be positioned at a key >= item.key. So it is sufficient to seek the
			// current l.rangeDelIter (since any range del iterators that will be provided by the
			// levelIter in the future cannot contain item.key). Also, it is possible that we
			// will encounter parts of the range delete that should be ignored -- we handle that
			// below.
			l.tombstone = rangedel.SeekGE(m.heap.cmp, l.rangeDelIter, item.key.UserKey, m.snapshot)
		}
		if l.tombstone.Empty() {
			continue
		}

		// Reasoning for correctness of untruncated tombstone handling when the untruncated
		// tombstone is at a higher level:
		// The iterator corresponding to this tombstone is still in the heap so it must be
		// positioned >= item.key. Which means the Largest key bound of the sstable containing this
		// tombstone is >= item.key. So the upper limit of this tombstone cannot be file-bounds-constrained
		// to < item.key. But it is possible that item.key < smallestUserKey, in which
		// case this tombstone should be ignored.
		//
		// Example 1:
		// sstable bounds [c#8, g#12] containing a tombstone [b, i)#7, and key is c#6. The
		// smallestUserKey is c, so we know the key is within the file bounds and the tombstone
		// [b, i) covers it.
		//
		// Example 2:
		// Same sstable bounds but key is b#10. The smallestUserKey is c, so the tombstone [b, i)
		// does not cover this key.
		//
		// For a tombstone at the same level as the key, the file bounds are trivially satisfied.
		if (l.smallestUserKey == nil || m.heap.cmp(l.smallestUserKey, item.key.UserKey) <= 0) &&
			l.tombstone.Contains(m.heap.cmp, item.key.UserKey) {
			if level < item.index {
				// We could also do m.seekGE(..., level + 1). The levels from
				// [level + 1, item.index) are already after item.key so seeking them may be
				// wasteful.

				// We can seek up to the min of largestUserKey and tombstone.End.
				//
				// Using example 1 above, we can seek to the smaller of g and i, which is g.
				//
				// Another example, where the sstable bounds are [c#8, i#InternalRangeDelSentinel],
				// and the tombstone is [b, i)#8. Seeking to i is correct since it is seeking up to
				// the exclusive bound of the tombstone. We do not need to look at
				// isLargestKeyRangeDelSentinel.
				//
				// Progress argument: Since this file is at a higher level than item.key we know
				// that the iterator in this file must be positioned within its bounds and at a key
				// X > item.key (otherwise it would be the min of the heap). It is not
				// possible for X.UserKey == item.key.UserKey, since it is incompatible with
				// X > item.key (a lower version cannot be in a higher sstable), so it must be that
				// X.UserKey > item.key.UserKey. Which means l.largestUserKey > item.key.UserKey.
				// We also know that l.tombstone.End > item.key.UserKey. So the min of these,
				// seekKey, computed below, is > item.key.UserKey, so the call to seekGE() will
				// make forward progress.
				seekKey := l.tombstone.End
				if l.largestUserKey != nil && m.heap.cmp(l.largestUserKey, seekKey) < 0 {
					seekKey = l.largestUserKey
				}
				m.seekGE(seekKey, item.index)
				return true
			}
			if l.tombstone.Deletes(item.key.SeqNum()) {
				m.nextEntry(item)
				return true
			}
		}
	}
	return false
}

// Starting from the current entry, finds the first (next) entry that can be returned.
func (m *mergingIter) findNextEntry() (*InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		if m.isNextEntryDeleted(item) {
			continue
		}
		if item.key.Visible(m.snapshot) &&
			(item.key.Kind() != InternalKeyKindRangeDelete || !m.elideRangeTombstones) {
			return &item.key, item.value
		}
		m.nextEntry(item)
	}
	return nil, nil
}

// Steps to the prev entry. item is the current top item in the heap.
func (m *mergingIter) prevEntry(item *mergingIterItem) {
	l := &m.levels[item.index]
	oldTopLevel := item.index
	oldRangeDelIter := l.rangeDelIter
	if l.iterKey, l.iterValue = l.iter.Prev(); l.iterKey != nil {
		item.key, item.value = *l.iterKey, l.iterValue
		if m.heap.len() > 1 {
			m.heap.fix(0)
		}
		if l.rangeDelIter != oldRangeDelIter && l.rangeDelIter != nil {
			// The rangeDelIter changed which indicates that the l.iter moved to the
			// previous sstable. We have to update the tombstone for oldTopLevel as
			// well.
			oldTopLevel--
		}
	} else {
		m.err = l.iter.Error()
		if m.err == nil {
			m.heap.pop()
		}
	}

	// The cached tombstones are only valid for the levels
	// [0,oldTopLevel]. Updated the cached tombstones for any levels in the range
	// [oldTopLevel+1,heap[0].index].
	m.initMaxRangeDelIters(oldTopLevel)
}

// isPrevEntryDeleted() starts from the current entry (as the prev entry) and if it is deleted,
// moves the iterators backward as needed and returns true, else it returns false. item is the top
// item in the heap.
func (m *mergingIter) isPrevEntryDeleted(item *mergingIterItem) bool {
	// Look for a range deletion tombstone containing item.key at higher
	// levels (level < item.index). If we find such a range tombstone we know
	// it deletes the key in the current level. Also look for a range
	// deletion at the current level (level == item.index). If we find such a
	// range deletion we need to check whether it is newer than the current
	// entry.
	for level := 0; level <= item.index; level++ {
		l := &m.levels[level]
		if l.rangeDelIter == nil || l.tombstone.Empty() {
			// If l.tombstone.Empty() is true, there are no further tombstones in the
			// current sstable in the current (reverse) iteration direction.
			continue
		}
		if m.heap.cmp(item.key.UserKey, l.tombstone.Start.UserKey) < 0 {
			// The current key is before the tombstone start key.
			//
			// NB: for the case that this l.rangeDelIter is provided by a levelIter we know that
			// the levelIter must be positioned at a key < item.key. So it is sufficient to seek the
			// current l.rangeDelIter (since any range del iterators that will be provided by the
			// levelIter in the future cannot contain item.key). Also, it is it is possible that we
			// will encounter parts of the range delete that should be ignored -- we handle that
			// below.
			l.tombstone = rangedel.SeekLE(m.heap.cmp, l.rangeDelIter, item.key.UserKey, m.snapshot)
		}
		if l.tombstone.Empty() {
			continue
		}

		// Reasoning for correctness of untruncated tombstone handling when the untruncated
		// tombstone is at a higher level:
		//
		// The iterator corresponding to this tombstone is still in the heap so it must be
		// positioned <= item.key. Which means the Smallest key bound of the sstable containing this
		// tombstone is <= item.key. So the lower limit of this tombstone cannot have been
		// file-bounds-constrained to > item.key. But it is possible that item.key >= Largest
		// key bound of this sstable, in which case this tombstone should be ignored.
		//
		// Example 1:
		// sstable bounds [c#8, g#12] containing a tombstone [b, i)#7, and key is f#6. The
		// largestUserKey is g, so we know the key is within the file bounds and the tombstone
		// [b, i) covers it.
		//
		// Example 2:
		// Same sstable but the key is g#6. This cannot happen since the [b, i)#7 untruncated
		// tombstone was involved in a compaction which must have had a file to the right of this
		// sstable that is part of the same atomic compaction group for future compactions. That
		// file must have bounds that cover g#6 and this levelIter must be at that file.
		//
		// Example 3:
		// sstable bounds [c#8, g#RangeDelSentinel] containing [b, i)#7 and the key is g#10.
		// This key is not deleted by this tombstone. We need to look at
		// isLargestUserKeyRangeDelSentinel.
		//
		// For a tombstone at the same level as the key, the file bounds are trivially satisfied.

		// Default to within bounds.
		withinLargestSSTableBound := true
		if l.largestUserKey != nil {
			cmpResult := m.heap.cmp(l.largestUserKey, item.key.UserKey)
			withinLargestSSTableBound = cmpResult > 0 || (cmpResult == 0 && !l.isLargestUserKeyRangeDelSentinel)
		}
		if withinLargestSSTableBound && l.tombstone.Contains(m.heap.cmp, item.key.UserKey) {
			if level < item.index {
				// We could also do m.seekLT(..., level + 1). The levels from
				// [level + 1, item.index) are already before item.key so seeking them may be
				// wasteful.

				// We can seek up to the max of smallestUserKey and tombstone.Start.UserKey.
				//
				// Using example 1 above, we can seek to the larger of c and b, which is c.
				//
				// Progress argument: We know that the iterator in this file is positioned within
				// its bounds and at a key X < item.key (otherwise it would be the max of the heap).
				// So smallestUserKey <= item.key.UserKey and we already know that
				// l.tombstone.Start.UserKey <= item.key.UserKey. So the seekKey computed below
				// is <= item.key.UserKey, and since we do a seekLT() we will make backwards
				// progress.
				seekKey := l.tombstone.Start.UserKey
				if l.smallestUserKey != nil && m.heap.cmp(l.smallestUserKey, seekKey) > 0 {
					seekKey = l.smallestUserKey
				}
				m.seekLT(seekKey, item.index)
				return true
			}
			if l.tombstone.Deletes(item.key.SeqNum()) {
				m.prevEntry(item)
				return true
			}
		}
	}
	return false
}

// Starting from the current entry, finds the first (prev) entry that can be returned.
func (m *mergingIter) findPrevEntry() (*InternalKey, []byte) {
	for m.heap.len() > 0 && m.err == nil {
		item := &m.heap.items[0]
		if m.isPrevEntryDeleted(item) {
			continue
		}
		if item.key.Visible(m.snapshot) &&
			(item.key.Kind() != InternalKeyKindRangeDelete || !m.elideRangeTombstones) {
			return &item.key, item.value
		}
		m.prevEntry(item)
	}
	return nil, nil
}

// Seeks levels >= level to >= key. Additionally uses range tombstones to extend the seeks.
func (m *mergingIter) seekGE(key []byte, level int) {
	// When seeking, we can use tombstones to adjust the key we seek to on each
	// level. Consider the series of range tombstones:
	//
	//   1: a---e
	//   2:    d---h
	//   3:       g---k
	//   4:          j---n
	//   5:             m---q
	//
	// If we SeekGE("b") we also find the tombstone "b" resides within in the
	// first level which is [a,e). Regardless of whether this tombstone deletes
	// "b" in that level, we know it deletes "b" in all lower levels, so we
	// adjust the search key in the next level to the tombstone end key "e". We
	// then SeekGE("e") in the second level and find the corresponding tombstone
	// [d,h). This process continues and we end up seeking for "h" in the 3rd
	// level, "k" in the 4th level and "n" in the last level.
	//
	// TODO(peter,rangedel): In addition to the above we can delay seeking a
	// level (and any lower levels) when the current iterator position is
	// contained within a range tombstone at a higher level.

	for ; level < len(m.levels); level++ {
		if raceEnabled && m.lower != nil && m.heap.cmp(key, m.lower) < 0 {
			m.logger.Fatalf("mergingIter: lower bound violation: %s < %s\n%s", key, m.lower, debug.Stack())
		}

		l := &m.levels[level]
		if m.prefix != nil {
			l.iterKey, l.iterValue = l.iter.SeekPrefixGE(m.prefix, key)
		} else {
			l.iterKey, l.iterValue = l.iter.SeekGE(key)
		}

		if rangeDelIter := l.rangeDelIter; rangeDelIter != nil {
			// The level has a range-del iterator. Find the tombstone containing
			// the search key.
			//
			// For untruncated tombstones that are possibly file-bounds-constrained, we are using a
			// levelIter which will set smallestUserKey and largestUserKey. Since the levelIter
			// is at this file we know that largestUserKey >= key, so we know that the
			// tombstone we find cannot be file-bounds-constrained in its upper bound to something < key.
			// We do need to  compare with smallestUserKey to ensure that the tombstone is not
			// file-bounds-constrained in its lower bound.
			//
			// See the detailed comments in isNextEntryDeleted() on why similar containment and
			// seeking logic is correct. The subtle difference here is that key is a user key,
			// so we can have a sstable with bounds [c#8, i#InternalRangeDelSentinel], and the
			// tombstone is [b, k)#8 and the seek key is i: levelIter.SeekGE(i) will move past
			// this sstable since it realizes the largest key is a InternalRangeDelSentinel.
			tombstone := rangedel.SeekGE(m.heap.cmp, rangeDelIter, key, m.snapshot)
			if !tombstone.Empty() && tombstone.Contains(m.heap.cmp, key) &&
				(l.smallestUserKey == nil || m.heap.cmp(l.smallestUserKey, key) <= 0) {
				// NB: Based on the comment above l.largestUserKey >= key, and based on the
				// containment condition tombstone.End > key, so the assignment to key results
				// in a monotonically non-decreasing key across iterations of this loop.
				//
				// The adjustment of key here can only move it to a larger key. Since
				// the caller of seekGE guaranteed that the original key was greater
				// than or equal to m.lower, the new key will continue to be greater
				// than or equal to m.lower.
				if l.largestUserKey != nil &&
					m.heap.cmp(l.largestUserKey, tombstone.End) < 0 {
					// Truncate the tombstone for seeking purposes. Note that this can over-truncate
					// but that is harmless for this seek optimization.
					key = l.largestUserKey
				} else {
					key = tombstone.End
				}
			}
		}
	}

	m.initMinHeap()
}

// SeekGE implements base.InternalIterator.SeekGE. Note that SeekGE only checks
// the upper bound. It is up to the caller to ensure that key is greater than
// or equal to the lower bound.
func (m *mergingIter) SeekGE(key []byte) (*InternalKey, []byte) {
	m.prefix = nil
	m.seekGE(key, 0 /* start level */)
	return m.findNextEntry()
}

// SeekPrefixGE implements base.InternalIterator.SeekPrefixGE. Note that
// SeekPrefixGE only checks the upper bound. It is up to the caller to ensure
// that key is greater than or equal to the lower bound.
func (m *mergingIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	m.prefix = prefix
	m.seekGE(key, 0 /* start level */)
	return m.findNextEntry()
}

// Seeks levels >= level to < key. Additionally uses range tombstones to extend the seeks.
func (m *mergingIter) seekLT(key []byte, level int) {
	// See the comment in seekGE regarding using tombstones to adjust the seek
	// target per level.
	m.prefix = nil
	for ; level < len(m.levels); level++ {
		if raceEnabled && m.upper != nil && m.heap.cmp(key, m.upper) > 0 {
			m.logger.Fatalf("mergingIter: upper bound violation: %s > %s\n%s", key, m.upper, debug.Stack())
		}

		l := &m.levels[level]
		l.iterKey, l.iterValue = l.iter.SeekLT(key)

		if rangeDelIter := l.rangeDelIter; rangeDelIter != nil {
			// The level has a range-del iterator. Find the tombstone containing
			// the search key.
			//
			// For untruncated tombstones that are possibly file-bounds-constrained we are using a
			// levelIter which will set smallestUserKey and largestUserKey. Since the levelIter
			// is at this file we know that smallestUserKey <= key, so we know that the
			// tombstone we find cannot be file-bounds-constrained in its lower bound to something > key.
			// We do need to  compare with largestUserKey to ensure that the tombstone is not
			// file-bounds-constrained in its upper bound.
			//
			// See the detailed comments in isPrevEntryDeleted() on why similar containment and
			// seeking logic is correct.

			// Default to within bounds.
			withinLargestSSTableBound := true
			if l.largestUserKey != nil {
				cmpResult := m.heap.cmp(l.largestUserKey, key)
				withinLargestSSTableBound = cmpResult > 0 || (cmpResult == 0 && !l.isLargestUserKeyRangeDelSentinel)
			}

			tombstone := rangedel.SeekLE(m.heap.cmp, rangeDelIter, key, m.snapshot)
			if !tombstone.Empty() && tombstone.Contains(m.heap.cmp, key) && withinLargestSSTableBound {
				// NB: Based on the comment above l.smallestUserKey <= key, and based
				// on the containment condition tombstone.Start.UserKey <= key, so the
				// assignment to key results in a monotonically non-increasing key
				// across iterations of this loop.
				//
				// The adjustment of key here can only move it to a smaller key. Since
				// the caller of seekLT guaranteed that the original key was less than
				// or equal to m.upper, the new key will continue to be less than or
				// equal to m.upper.
				if l.smallestUserKey != nil &&
					m.heap.cmp(l.smallestUserKey, tombstone.Start.UserKey) >= 0 {
					// Truncate the tombstone for seeking purposes. Note that this can over-truncate
					// but that is harmless for this seek optimization.
					key = l.smallestUserKey
				} else {
					key = tombstone.Start.UserKey
				}
			}
		}
	}

	m.initMaxHeap()
}

// SeekLT implements base.InternalIterator.SeekLT. Note that SeekLT only checks
// the lower bound. It is up to the caller to ensure that key is less than the
// upper bound.
func (m *mergingIter) SeekLT(key []byte) (*InternalKey, []byte) {
	m.prefix = nil
	m.seekLT(key, 0 /* start level */)
	return m.findPrevEntry()
}

// First implements base.InternalIterator.First. Note that First only checks
// the upper bound. It is up to the caller to ensure that key is greater than
// or equal to the lower bound (e.g. via a call to SeekGE(lower)).
func (m *mergingIter) First() (*InternalKey, []byte) {
	m.prefix = nil
	m.heap.items = m.heap.items[:0]
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.First()
	}
	m.initMinHeap()
	return m.findNextEntry()
}

// Last implements base.InternalIterator.Last. Note that Last only checks the
// lower bound. It is up to the caller to ensure that key is less than the
// upper bound (e.g. via a call to SeekLT(upper))
func (m *mergingIter) Last() (*InternalKey, []byte) {
	m.prefix = nil
	for i := range m.levels {
		l := &m.levels[i]
		l.iterKey, l.iterValue = l.iter.Last()
	}
	m.initMaxHeap()
	return m.findPrevEntry()
}

func (m *mergingIter) Next() (*InternalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != 1 {
		m.switchToMinHeap()
		return m.findNextEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.nextEntry(&m.heap.items[0])
	return m.findNextEntry()
}

func (m *mergingIter) Prev() (*InternalKey, []byte) {
	if m.err != nil {
		return nil, nil
	}

	if m.dir != -1 {
		if m.prefix != nil {
			m.err = errors.New("pebble: unsupported reverse prefix iteration")
			return nil, nil
		}
		m.switchToMaxHeap()
		return m.findPrevEntry()
	}

	if m.heap.len() == 0 {
		return nil, nil
	}

	m.prevEntry(&m.heap.items[0])
	return m.findPrevEntry()
}

func (m *mergingIter) Error() error {
	if m.heap.len() == 0 || m.err != nil {
		return m.err
	}
	return m.levels[m.heap.items[0].index].iter.Error()
}

func (m *mergingIter) Close() error {
	for i := range m.levels {
		iter := m.levels[i].iter
		if err := iter.Close(); err != nil && m.err == nil {
			m.err = err
		}
		if rangeDelIter := m.levels[i].rangeDelIter; rangeDelIter != nil {
			if err := rangeDelIter.Close(); err != nil && m.err == nil {
				m.err = err
			}
		}
	}
	m.levels = nil
	m.heap.items = nil
	return m.err
}

func (m *mergingIter) SetBounds(lower, upper []byte) {
	m.prefix = nil
	m.lower = lower
	m.upper = upper
	for i := range m.levels {
		m.levels[i].iter.SetBounds(lower, upper)
	}
	m.heap.clear()
}

func (m *mergingIter) DebugString() string {
	var buf bytes.Buffer
	sep := ""
	for m.heap.len() > 0 {
		item := m.heap.pop()
		fmt.Fprintf(&buf, "%s%s", sep, item.key)
		sep = " "
	}
	if m.dir == 1 {
		m.initMinHeap()
	} else {
		m.initMaxHeap()
	}
	return buf.String()
}
