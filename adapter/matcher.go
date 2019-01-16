package adapter

import (
	"github.com/prometheus/tsdb/labels"
)

type notMatcher struct {
	labels.Matcher
}

func (m *notMatcher) Matches(v string) bool { return !m.Matcher.Matches(v) }

type notRegexpMatcher struct {
	labels.Matcher
}

func (m *notRegexpMatcher) Matches(v string) bool { return !m.Matcher.Matches(v) }