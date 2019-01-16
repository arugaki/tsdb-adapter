package adapter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"github.com/go-kit/kit/log"
	"time"
	"tsdb-adpter/config"
)

type Adapter struct {
	db     *tsdb.DB
	logger log.Logger
}

func NewAdapter(l log.Logger, c *config.Config) (*Adapter, error) {
	if c.MinBlockDuration > c.MaxBlockDuration {
		c.MaxBlockDuration = c.MinBlockDuration
	}
	// Start with smallest block duration and create exponential buckets until the exceed the
	// configured maximum block duration.
	rngs := tsdb.ExponentialBlockRanges(int64(time.Duration(c.MinBlockDuration).Seconds()*1000), 10, 3)

	for i, v := range rngs {
		if v > int64(time.Duration(c.MaxBlockDuration).Seconds()*1000) {
			rngs = rngs[:i]
			break
		}
	}

	db, err := tsdb.Open(c.Path, l, prometheus.DefaultRegisterer, &tsdb.Options{
		WALFlushInterval:  10 * time.Second,
		RetentionDuration: uint64(time.Duration(c.Retention).Seconds() * 1000),
		BlockRanges:       rngs,
		NoLockfile:        true,
	})

	if err != nil {
		l.Log("error is ", err)
		return nil, err
	}

	return &Adapter{db, l}, nil
}

func (a *Adapter) RemoteReader(querys prompb.ReadRequest) *prompb.ReadResponse {
	//query
	query := querys.Queries[0]
	startTime := query.StartTimestampMs
	endTime := query.EndTimestampMs
	matchers := query.Matchers
	a.logger.Log("Query:", startTime, endTime, matchers)

	ms := convertLabelMatcher(matchers)

	var queryResult prompb.QueryResult
	queryResult.Timeseries = a.getTimeSeries(startTime, endTime, ms)
	var queryResults []*prompb.QueryResult
	queryResults = append(queryResults, &queryResult)
	var resp prompb.ReadResponse
	resp.Results = queryResults

	return &resp
}

func (a *Adapter) RemoteWriter(querys prompb.WriteRequest)  {

}


func (a *Adapter) getTimeSeries(mint, maxt int64, labels []labels.Matcher) []*prompb.TimeSeries {

	tss := make([]*prompb.TimeSeries, 0)
	var v float64
	var t int64
	var ts *prompb.TimeSeries

	q, err := a.db.Querier(mint, maxt)
	if err != nil {
		a.logger.Log("error is ", err)
		return nil
	}

	series, err := q.Select(labels...)
	if err != nil {
		a.logger.Log("error is ", err)
		return nil
	}

	for ss := series.At(); ss != nil;{
		ts = nil
		lbs := ss.Labels()
		for _, lb := range lbs {
			ts.Labels = append(ts.Labels, &prompb.Label{lb.Name, lb.Value})
		}

		si := ss.Iterator()
		for t, v = si.At();;{
			ts.Samples = append(ts.Samples, prompb.Sample{v, t})

			if si.Next() == false {
				break
			}
		}
	}

	return tss
}

func convertLabelMatcher(matchers []*prompb.LabelMatcher) []labels.Matcher {
	lbs := make([]labels.Matcher, len(matchers))
	var mat labels.Matcher
	for _, m := range matchers {
		switch m.Type {
		case prompb.LabelMatcher_EQ:
			mat = labels.NewEqualMatcher(m.Name, m.Value)
			lbs = append(lbs, mat)
		case prompb.LabelMatcher_RE:
			mat, err := labels.NewRegexpMatcher(m.Name, m.Value)
			if err != nil {
			}
			lbs = append(lbs, mat)
		case prompb.LabelMatcher_NEQ:
			mat :=  &notMatcher{labels.NewEqualMatcher(m.Name, m.Value)}
			lbs = append(lbs, mat)
		case prompb.LabelMatcher_NRE:
			m, err := labels.NewRegexpMatcher(m.Name, m.Value)
			if err != nil {

			}
			mat :=  &notRegexpMatcher{m}
			lbs = append(lbs, mat)
		}

	}

	return lbs
}