// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// FindInlinableConstants returns the set of input columns that are synthesized
// constant value expressions: ConstOp, TrueOp, FalseOp, or NullOp. Constant
// value expressions can often be inlined into referencing expressions. Only
// Project and Values operators synthesize constant value expressions.
func (c *CustomFuncs) FindInlinableConstants(input memo.RelExpr) opt.ColSet {
	return memo.FindInlinableConstants(input)
}

// InlineProjectionConstants recursively searches each projection expression and
// replaces any references to input columns that are constant. It returns a new
// Projections list containing the replaced expressions.
func (c *CustomFuncs) InlineProjectionConstants(
	projections memo.ProjectionsExpr, input memo.RelExpr, constCols opt.ColSet,
) memo.ProjectionsExpr {
	newProjections := make(memo.ProjectionsExpr, len(projections))
	for i := range projections {
		item := &projections[i]
		newProjections[i] = c.f.ConstructProjectionsItem(
			c.inlineConstants(item.Element, input, constCols).(opt.ScalarExpr),
			item.Col,
		)
	}
	return newProjections
}

// InlineFilterConstants recursively searches each filter expression and
// replaces any references to input columns that are constant. It returns a new
// Filters list containing the replaced expressions.
func (c *CustomFuncs) InlineFilterConstants(
	filters memo.FiltersExpr, input memo.RelExpr, constCols opt.ColSet,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		item := &filters[i]
		newFilters[i] = c.f.ConstructFiltersItem(
			c.inlineConstants(item.Condition, input, constCols).(opt.ScalarExpr),
		)
	}
	return newFilters
}

// inlineConstants recursively searches the given expression and replaces any
// references to input columns that are constant. It returns the replaced
// expression.
func (c *CustomFuncs) inlineConstants(
	e opt.Expr, input memo.RelExpr, constCols opt.ColSet,
) opt.Expr {
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			if constCols.Contains(t.Col) {
				return memo.ExtractColumnFromProjectOrValues(input, t.Col)
			}
			return t
		}
		return c.f.Replace(e, replace)
	}
	return replace(e)
}

// HasDuplicateRefs returns true if the target projection expressions or
// passthrough columns reference any column in the given target set more than
// one time, or if the projection expressions contain a correlated subquery.
// For example:
//
//   SELECT x+1, x+2, y FROM a
//
// HasDuplicateRefs would be true, since the x column is referenced twice.
//
// Correlated subqueries are disallowed since it introduces additional
// complexity for a case that's not too important for inlining. Also, skipping
// correlated subqueries minimizes expensive searching in deep trees.
func (c *CustomFuncs) HasDuplicateRefs(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, targetCols opt.ColSet,
) bool {
	// Passthrough columns that reference a target column count as refs.
	refs := passthrough.Intersection(targetCols)
	for i := range projections {
		item := &projections[i]
		if item.ScalarProps().HasCorrelatedSubquery {
			// Don't traverse the expression tree if there is a correlated subquery.
			return true
		}

		// When a target column reference is found, add it to the refs set. If
		// the set already contains a reference to that column, then there is a
		// duplicate. findDupRefs returns true if the subtree contains at least
		// one duplicate.
		var findDupRefs func(e opt.Expr) bool
		findDupRefs = func(e opt.Expr) bool {
			switch t := e.(type) {
			case *memo.VariableExpr:
				// Ignore references to non-target columns.
				if !targetCols.Contains(t.Col) {
					return false
				}

				// Count Variable references.
				if refs.Contains(t.Col) {
					return true
				}
				refs.Add(t.Col)
				return false

			case memo.RelExpr:
				// We know that this is not a correlated subquery since
				// HasCorrelatedSubquery was already checked above. Uncorrelated
				// subqueries never have references.
				return false
			}

			for i, n := 0, e.ChildCount(); i < n; i++ {
				if findDupRefs(e.Child(i)) {
					return true
				}
			}
			return false
		}

		if findDupRefs(item.Element) {
			return true
		}
	}
	return false
}

// CanInlineProjections returns true if all projection expressions can be
// inlined. See CanInline for details.
func (c *CustomFuncs) CanInlineProjections(projections memo.ProjectionsExpr) bool {
	for i := range projections {
		if !c.CanInline(projections[i].Element) {
			return false
		}
	}
	return true
}

// CanInline returns true if the given expression consists only of "simple"
// operators like Variable, Const, Eq, and Plus. These operators are assumed to
// be relatively inexpensive to evaluate, and therefore potentially evaluating
// them multiple times is not a big concern.
func (c *CustomFuncs) CanInline(scalar opt.ScalarExpr) bool {
	switch scalar.Op() {
	case opt.AndOp, opt.OrOp, opt.NotOp, opt.TrueOp, opt.FalseOp,
		opt.EqOp, opt.NeOp, opt.LeOp, opt.LtOp, opt.GeOp, opt.GtOp,
		opt.IsOp, opt.IsNotOp, opt.InOp, opt.NotInOp,
		opt.VariableOp, opt.ConstOp, opt.NullOp,
		opt.PlusOp, opt.MinusOp, opt.MultOp:

		// Recursively verify that children are also inlinable.
		for i, n := 0, scalar.ChildCount(); i < n; i++ {
			if !c.CanInline(scalar.Child(i).(opt.ScalarExpr)) {
				return false
			}
		}
		return true
	}
	return false
}

// VirtualColumns returns the set of columns in the scanPrivate's table that are
// virtual computed columns.
func (c *CustomFuncs) VirtualColumns(scanPrivate *memo.ScanPrivate) opt.ColSet {
	tabMeta := c.mem.Metadata().TableMeta(scanPrivate.Table)
	return tabMeta.VirtualComputedColumns()
}

// InlinableVirtualColumnFilters returns a new filters expression containing any
// of the given filters that meet the criteria:
//
//   1. The filter has references to any of the columns in virtualColumns.
//   2. The filter is not a correlated subquery.
//
func (c *CustomFuncs) InlinableVirtualColumnFilters(
	filters memo.FiltersExpr, virtualColumns opt.ColSet,
) (inlinableFilters memo.FiltersExpr) {
	for i := range filters {
		item := &filters[i]

		// Do not inline a filter if it has a correlated subquery or it does not
		// reference a virtual column.
		if item.ScalarProps().HasCorrelatedSubquery || !item.ScalarProps().OuterCols.Intersects(virtualColumns) {
			continue
		}

		// Initialize inlinableFilters lazily.
		if inlinableFilters == nil {
			inlinableFilters = make(memo.FiltersExpr, 0, len(filters)-i)
		}

		inlinableFilters = append(inlinableFilters, *item)
	}
	return inlinableFilters
}

// InlineSelectProject searches the filter conditions for any variable
// references to columns from the given projections expression. Each variable is
// replaced by the corresponding inlined projection expression.
func (c *CustomFuncs) InlineSelectProject(
	filters memo.FiltersExpr, projections memo.ProjectionsExpr,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		item := &filters[i]
		newFilters[i] = c.f.ConstructFiltersItem(
			c.inlineProjections(item.Condition, projections).(opt.ScalarExpr),
		)
	}
	return newFilters
}

// InlineProjectProject searches the projection expressions for any variable
// references to columns from the given input (which must be a Project
// operator). Each variable is replaced by the corresponding inlined projection
// expression.
func (c *CustomFuncs) InlineProjectProject(
	innerProject *memo.ProjectExpr, projections memo.ProjectionsExpr, passthrough opt.ColSet,
) memo.RelExpr {
	innerProjections := innerProject.Projections

	newProjections := make(memo.ProjectionsExpr, len(projections))
	for i := range projections {
		item := &projections[i]

		newProjections[i] = c.f.ConstructProjectionsItem(
			c.inlineProjections(item.Element, innerProjections).(opt.ScalarExpr),
			item.Col,
		)
	}

	// Add any outer passthrough columns that refer to inner synthesized columns.
	newPassthrough := passthrough
	if !newPassthrough.Empty() {
		for i := range innerProjections {
			item := &innerProjections[i]
			if newPassthrough.Contains(item.Col) {
				newProjections = append(newProjections, *item)
				newPassthrough.Remove(item.Col)
			}
		}
	}

	return c.f.ConstructProject(innerProject.Input, newProjections, newPassthrough)
}

// Recursively walk the tree looking for references to projection expressions
// that need to be replaced.
func (c *CustomFuncs) inlineProjections(e opt.Expr, projections memo.ProjectionsExpr) opt.Expr {
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.VariableExpr:
			for i := range projections {
				if projections[i].Col == t.Col {
					return projections[i].Element
				}
			}
			return t

		case memo.RelExpr:
			if !c.OuterCols(t).Empty() {
				// Should have prevented this in HasDuplicateRefs/HasCorrelatedSubquery.
				panic(errors.AssertionFailedf("cannot inline references within correlated subqueries"))
			}

			// No projections references possible, since there are no outer cols.
			return t
		}

		return c.f.Replace(e, replace)
	}

	return replace(e)
}

func (c *CustomFuncs) extractVarEqualsConst(
	e opt.Expr,
) (ok bool, left *memo.VariableExpr, right *memo.ConstExpr) {
	if eq, ok := e.(*memo.EqExpr); ok {
		if l, ok := eq.Left.(*memo.VariableExpr); ok {
			if r, ok := eq.Right.(*memo.ConstExpr); ok {
				return true, l, r
			}
		}
	}
	return false, nil, nil
}

// CanInlineConstVar returns true if there is an opportunity in the filters to
// inline a variable restricted to be a constant, as in:
//   SELECT * FROM foo WHERE a = 4 AND a IN (1, 2, 3, 4).
// =>
//   SELECT * FROM foo WHERE a = 4 AND 4 IN (1, 2, 3, 4).
func (c *CustomFuncs) CanInlineConstVar(f memo.FiltersExpr) bool {
	// usedIndices tracks the set of filter indices we've used to infer constant
	// values, so we don't inline into them.
	var usedIndices util.FastIntSet
	// fixedCols is the set of columns that the filters restrict to be a constant
	// value.
	var fixedCols opt.ColSet
	for i := range f {
		if ok, l, _ := c.extractVarEqualsConst(f[i].Condition); ok {
			colType := c.mem.Metadata().ColumnMeta(l.Col).Type
			if colinfo.CanHaveCompositeKeyEncoding(colType) {
				// TODO(justin): allow inlining if the check we're doing is oblivious
				// to composite-ness.
				continue
			}
			if !fixedCols.Contains(l.Col) {
				fixedCols.Add(l.Col)
				usedIndices.Add(i)
			}
		}
	}
	for i := range f {
		if usedIndices.Contains(i) {
			continue
		}
		if f[i].ScalarProps().OuterCols.Intersects(fixedCols) {
			return true
		}
	}
	return false
}

// InlineConstVar performs the inlining detected by CanInlineConstVar.
func (c *CustomFuncs) InlineConstVar(f memo.FiltersExpr) memo.FiltersExpr {
	// usedIndices tracks the set of filter indices we've used to infer constant
	// values, so we don't inline into them.
	var usedIndices util.FastIntSet
	// fixedCols is the set of columns that the filters restrict to be a constant
	// value.
	var fixedCols opt.ColSet
	// vals maps columns which are restricted to be constant to the value they
	// are restricted to.
	vals := make(map[opt.ColumnID]opt.ScalarExpr)
	for i := range f {
		if ok, v, e := c.extractVarEqualsConst(f[i].Condition); ok {
			colType := c.mem.Metadata().ColumnMeta(v.Col).Type
			if colinfo.CanHaveCompositeKeyEncoding(colType) {
				continue
			}
			if _, ok := vals[v.Col]; !ok {
				vals[v.Col] = e
				fixedCols.Add(v.Col)
				usedIndices.Add(i)
			}
		}
	}

	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		if t, ok := nd.(*memo.VariableExpr); ok {
			if e, ok := vals[t.Col]; ok {
				return e
			}
		}
		return c.f.Replace(nd, replace)
	}

	result := make(memo.FiltersExpr, len(f))
	for i := range f {
		inliningNeeded := f[i].ScalarProps().OuterCols.Intersects(fixedCols)
		// Don't inline if we used this position to infer a constant value, or if
		// the expression doesn't contain any fixed columns.
		if usedIndices.Contains(i) || !inliningNeeded {
			result[i] = f[i]
		} else {
			newCondition := replace(f[i].Condition).(opt.ScalarExpr)
			result[i] = c.f.ConstructFiltersItem(newCondition)
		}
	}
	return result
}

func (c *CustomFuncs) extractVarEqOrGtOrGeConst(
	e opt.Expr,
) (ok bool, left *memo.VariableExpr, right *memo.ConstExpr, operator opt.Operator) {
	switch t := e.(type) {
	case *memo.EqExpr, *memo.GtExpr, *memo.GeExpr, *memo.LtExpr, *memo.LeExpr:
		if l, ok := e.Child(0).(*memo.VariableExpr); ok {
			if r, ok := e.Child(1).(*memo.ConstExpr); ok {
				return true, l, r, e.Op()
			}
		}
	case *memo.RangeExpr:
		if and, ok := t.And.(*memo.AndExpr); ok {
			if ok, l, r, op := c.extractVarEqOrGtOrGeConst(and.Left); ok {
				return ok, l, r, op
			}
			return c.extractVarEqOrGtOrGeConst(and.Right)
		}
	}
	return false, nil, nil, 0
}

// CanInlineVarEqOrGtOrGeConst returns true if there is an opportunity in the filters to
// inline a variable, as in:
// between  =、 <=、 >=、 <、 >。
//   SELECT * FROM foo WHERE a = 4 AND  a > b
// =>
//   SELECT * FROM foo WHERE a = 4 AND 4 > b.
func (c *CustomFuncs) CanInlineVarEqOrGtOrGeConst(f memo.FiltersExpr) bool {
	// usedIndices tracks the set of filter indices we've used to infer constant
	// values or its type is obvious, so we don't inline into them.
	var usedIndicesOrObliviousCol util.FastIntSet

	// fixedOpColsMap is the map of columns that the filters restrict to be a constant
	// value and operators.
	fixedOpColsMap := make(map[opt.Operator]*opt.ColSet)
	fixedOpColsMap[opt.EqOp] = &opt.ColSet{}
	fixedOpColsMap[opt.GeOp] = &opt.ColSet{}
	fixedOpColsMap[opt.GtOp] = &opt.ColSet{}
	fixedOpColsMap[opt.LeOp] = &opt.ColSet{}
	fixedOpColsMap[opt.LtOp] = &opt.ColSet{}

	for i := range f {
		if ok, l, _, op := c.extractVarEqOrGtOrGeConst(f[i].Condition); ok {
			colType := c.mem.Metadata().ColumnMeta(l.Col).Type
			if colinfo.CanHaveCompositeKeyEncoding(colType) {
				// TODO(justin): allow inlining if the check we're doing is oblivious
				// to composite-ness.
				usedIndicesOrObliviousCol.Add(i)
				continue
			}
			// If the given filters contain both a > 10 and a = 11 for column "a", only a = 11 will be passed.
			// in this case, filter contains Eq and the other compare op, the Gt or Ge will not be handled.
			// Here you can eliminate the useless conditions and take the most suitable one.
			// We can also use another rule to implement it, more appropriately.

			if !fixedOpColsMap[op].Contains(l.Col) {
				fixedOpColsMap[op].Add(l.Col)
				usedIndicesOrObliviousCol.Add(i)
			}
		}
	}
	var fixedOpColsMapValue opt.ColSet
	for _, v := range fixedOpColsMap {
		fixedOpColsMapValue.UnionWith(*v)
	}

	for i := range f {
		inliningNeeded := f[i].ScalarProps().OuterCols.Intersects(fixedOpColsMapValue)
		if usedIndicesOrObliviousCol.Contains(i) || !inliningNeeded {
			continue
		}
		// 这种判断有缺陷，如果 a > b and a > 9 and b > 10, 不对 a > b 做处理，这种是没问题的
		// 但是如果出现 a > b and c > b and b < 10 and c = 9, 对 c > b
		// 这个表达式就会不做处理了，但是事实上应该为闭包增加传递 b < 9。
		if outCols := f[i].ScalarProps().OuterCols; outCols.SubsetOf(fixedOpColsMapValue) {
			continue
		}
		if outCols := f[i].ScalarProps().OuterCols; outCols.Intersects(fixedOpColsMapValue) {
			return true
		}
	}
	return false
}

// InlineVarEqOrGtOrGeConst performs the inlining detected by CanInlineConstVar.
func (c *CustomFuncs) InlineVarEqOrGtOrGeConst(f memo.FiltersExpr) memo.FiltersExpr {
	// usedIndicesOrObliviousCol tracks the set of filter indices we've used to infer constant
	// values or its type is obvious, so we don't inline into them.
	var usedIndicesOrObliviousCol util.FastIntSet

	// fixedOpColsMap is the map of columns that the filters restrict to be a constant
	// value and operators.
	fixedOpColsMap := make(map[opt.Operator]*opt.ColSet)
	fixedOpColsMap[opt.EqOp] = &opt.ColSet{}
	fixedOpColsMap[opt.GeOp] = &opt.ColSet{}
	fixedOpColsMap[opt.GtOp] = &opt.ColSet{}
	fixedOpColsMap[opt.LeOp] = &opt.ColSet{}
	fixedOpColsMap[opt.LtOp] = &opt.ColSet{}
	vals := make(map[opt.ColumnID]opt.ScalarExpr)
	for i := range f {
		if ok, v, e, op := c.extractVarEqOrGtOrGeConst(f[i].Condition); ok {
			colType := c.mem.Metadata().ColumnMeta(v.Col).Type
			if colinfo.CanHaveCompositeKeyEncoding(colType) {
				usedIndicesOrObliviousCol.Add(i)
				continue
			}
			if _, ok := vals[v.Col]; !ok {
				vals[v.Col] = e
				fixedOpColsMap[op].Add(v.Col)
				usedIndicesOrObliviousCol.Add(i)
			}
		}
	}

	result := make(memo.FiltersExpr, len(f))
	copy(result, f)

	var fixedOpColsMapValue opt.ColSet
	for _, v := range fixedOpColsMap {
		fixedOpColsMapValue.UnionWith(*v)
	}
	for i := range f {
		inliningNeeded := f[i].ScalarProps().OuterCols.Intersects(fixedOpColsMapValue)
		// Don't inline if we used this position to infer a constant value, or if
		// the expression doesn't contain any fixed columns.
		if usedIndicesOrObliviousCol.Contains(i) || !inliningNeeded {
			continue
		} else if outCols := f[i].ScalarProps().OuterCols; outCols.SubsetOf(fixedOpColsMapValue) {
			continue
		} else {
			rfs := c.constructInlineFiltersItem(f[i], fixedOpColsMap, vals)
			result = append(result, rfs...)
		}
	}
	return result
}

// 判断 闭包内的表达式 还有 闭包外的表达式 谓词传递的具体情况，构建具体表达式
// 等值传递除外，建议等值传递保留使用原来的方法和优化规则，如果这里再实现不知道会不会死循环
func (c *CustomFuncs) constructInlineFiltersItem(f memo.FiltersItem,
	fixedColsMap map[opt.Operator]*opt.ColSet, vals map[opt.ColumnID]opt.ScalarExpr) []memo.FiltersItem {
	var results memo.FiltersExpr
	if frs := c.constructInlineFiltersItemForCol(f, 0, fixedColsMap, vals); frs != nil {
		results = append(results, *frs...)
	}
	if frs := c.constructInlineFiltersItemForCol(f, 1, fixedColsMap, vals); frs != nil {
		results = append(results, *frs...)
	}
	return results
}

// constructInlineFiltersItemForCol colHasBeenFixed and colWillBeFixed are columns in a filter.
// colHasBeenFixed has its const value and was stored in fixedColsMap, while colWillBeFixed is
// only a part of current filter(the left one in filter).
// For example, a > b and b > 10, a is colWillBeFixed, b is colHasBeenFixed
// Meanwhile, for b > a and b < 10, will be handled as a < b and b < 10,
// a is colWillBeFixed and b is colHasBeenFixed
// 困惑之处：对于一个变量 var， 可能有多个[var op const]表达式
// 对于同一个变量 var 可以有多个op，同时也可能有多个const
// 基于以上认知，对于同一个变量 var 同一个op，也可能有多个const
// 因此要充分考虑这一点
func (c *CustomFuncs) constructInlineFiltersItemForCol(
	f memo.FiltersItem, needConverse int,
	fixedColsMap map[opt.Operator]*opt.ColSet, vals map[opt.ColumnID]opt.ScalarExpr) *memo.FiltersExpr {

	var res memo.FiltersItem
	var results memo.FiltersExpr

	colWillBeFixed, lok := f.Condition.Child(needConverse).(*memo.VariableExpr)
	colHasBeenFixed, rok := f.Condition.Child(1 - needConverse).(*memo.VariableExpr)
	if !lok || !rok {
		return nil
	}

	op := f.Condition.Op()

	reverseOption := func(op opt.Operator) opt.Operator {
		switch op {
		case opt.EqOp:
			return opt.EqOp
		case opt.LtOp:
			return opt.GtOp
		case opt.LeOp:
			return opt.GeOp
		case opt.GtOp:
			return opt.LtOp
		case opt.GeOp:
			return opt.LtOp
		}
		return opt.EqOp
	}

	if needConverse == 1 {
		op = reverseOption(op)
	}

	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		if t, ok := nd.(*memo.VariableExpr); ok {
			if e, ok := vals[t.Col]; ok {
				return e
			}
		}
		return c.f.Replace(nd, replace)
	}

	// Here you can eliminate the useless conditions and take the most suitable one.
	// We can also use another rule to implement it, more appropriately.
	switch op {
	case opt.EqOp:
		//if fixedEqCols := fixedColsMap[opt.EqOp]; fixedEqCols.Contains(colHasBeenFixed.Col) {
		//	return nil
		//} else {
		newCondition := replace(f.Condition).(opt.ScalarExpr)
		res = c.f.ConstructFiltersItem(newCondition)
		results = append(results, res)
		//}
	case opt.GtOp:
		if fixedEqCols, fixedGtCols, fixedGeCols := fixedColsMap[opt.EqOp], fixedColsMap[opt.GtOp], fixedColsMap[opt.GeOp]; fixedEqCols.Contains(colHasBeenFixed.Col) || fixedGtCols.Contains(colHasBeenFixed.Col) ||
			fixedGeCols.Contains(colHasBeenFixed.Col) {
			res = c.f.ConstructFiltersItem(c.f.ConstructGt(colWillBeFixed, vals[colHasBeenFixed.Col]))
			results = append(results, res)
		} else if fixedLeCols, fixedLtCols := fixedColsMap[opt.LeOp], fixedColsMap[opt.LtOp]; fixedLeCols.Contains(colHasBeenFixed.Col) || fixedLtCols.Contains(colHasBeenFixed.Col) {
			return nil
		}
	case opt.GeOp:
		if fixedEqCols, fixedGeCols := fixedColsMap[opt.EqOp], fixedColsMap[opt.GeOp]; fixedEqCols.Contains(colHasBeenFixed.Col) || fixedGeCols.Contains(colHasBeenFixed.Col) {
			res = c.f.ConstructFiltersItem(c.f.ConstructGe(colWillBeFixed, vals[colHasBeenFixed.Col]))
			results = append(results, res)
		} else if fixedGtCols := fixedColsMap[opt.GtOp]; fixedGtCols.Contains(colHasBeenFixed.Col) {
			res = c.f.ConstructFiltersItem(c.f.ConstructGt(colWillBeFixed, vals[colHasBeenFixed.Col]))
			results = append(results, res)
		} else if fixedLeCols, fixedLtCols := fixedColsMap[opt.LeOp], fixedColsMap[opt.LtOp]; fixedLeCols.Contains(colHasBeenFixed.Col) || fixedLtCols.Contains(colHasBeenFixed.Col) {
			return nil
		}
	case opt.LtOp:
		if fixedEqCols, fixedLeCols, fixedLtCols := fixedColsMap[opt.EqOp], fixedColsMap[opt.LeOp], fixedColsMap[opt.LtOp]; fixedEqCols.Contains(colHasBeenFixed.Col) || fixedLeCols.Contains(colHasBeenFixed.Col) ||
			fixedLtCols.Contains(colHasBeenFixed.Col) {
			res = c.f.ConstructFiltersItem(c.f.ConstructLt(colWillBeFixed, vals[colHasBeenFixed.Col]))
			results = append(results, res)
		} else if fixedGeCols, fixedGtCols := fixedColsMap[opt.GeOp], fixedColsMap[opt.GtOp]; fixedGeCols.Contains(colHasBeenFixed.Col) || fixedGtCols.Contains(colHasBeenFixed.Col) {
			return nil
		}
	case opt.LeOp:
		if fixedEqCols, fixedLeCols := fixedColsMap[opt.EqOp], fixedColsMap[opt.LeOp]; fixedEqCols.Contains(colHasBeenFixed.Col) || fixedLeCols.Contains(colHasBeenFixed.Col) {
			res = c.f.ConstructFiltersItem(c.f.ConstructLe(colWillBeFixed, vals[colHasBeenFixed.Col]))
			results = append(results, res)
		} else if fixedGeCols, fixedGtCols := fixedColsMap[opt.GeOp], fixedColsMap[opt.GtOp]; fixedGeCols.Contains(colHasBeenFixed.Col) || fixedGtCols.Contains(colHasBeenFixed.Col) {
			return nil
		} else if fixedLtCols := fixedColsMap[opt.LtOp]; fixedLtCols.Contains(colHasBeenFixed.Col) {
			res = c.f.ConstructFiltersItem(c.f.ConstructLt(colWillBeFixed, vals[colHasBeenFixed.Col]))
			results = append(results, res)
		}
	}
	return &results
}
