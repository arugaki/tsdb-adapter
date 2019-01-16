package adapter

import (
	"errors"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"time"
	"tsdb-adapter/config"
)

type Adapter struct {
	db     *tsdb.DB
	logger log.Logger
	cache  map[uint64]uint64
}

var errSampleLimit = errors.New("sample limit exceeded")

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
		level.Error(l).Log("msg", "open tsdb failed", "err", err)
		return nil, err
	}

	return &Adapter{db, l, make(map[uint64]uint64)}, nil
}

func (a *Adapter) RemoteReader(querys prompb.ReadRequest) *prompb.ReadResponse {
	//query
	var startTime, endTime int64
	var matchers []*prompb.LabelMatcher
	var queryResult prompb.QueryResult
	var queryResults []*prompb.QueryResult
	var resp prompb.ReadResponse
	var err error

	for _, query := range querys.Queries {
		startTime = query.StartTimestampMs
		endTime = query.EndTimestampMs
		matchers = query.Matchers
		ms := a.convertLabelMatcher(matchers)

		queryResult.Timeseries, err = a.getTimeSeries(startTime, endTime, ms)
		if err != nil {
			level.Debug(a.logger).Log("call", "getTimeSeries", "err", err)
			return nil
		}
		queryResults = append(queryResults, &queryResult)
	}

	resp.Results = queryResults
	return &resp
}

func (a *Adapter) RemoteWriter(querys prompb.WriteRequest)  {

	var hash uint64
	var lbls labels.Labels

	app := a.db.Appender()

	flag := true

	for _, ts := range querys.Timeseries {
		for _, sample := range ts.Samples {
			lbls = convertLabel(ts.Labels)
			hash = lbls.Hash()
			if ref, ok :=a.cache[hash]; ok {
				switch err := app.AddFast(ref, sample.Timestamp, sample.Value); err {
				case nil:
					continue
				case storage.ErrNotFound:
					level.Debug(a.logger).Log("msg", "Error not found", "err", err)
					flag = false
				case storage.ErrOutOfOrderSample:
					level.Debug(a.logger).Log("msg", "Out of order sample", "series", err)
					flag = false
					continue
				case storage.ErrDuplicateSampleForTimestamp:
					level.Debug(a.logger).Log("msg", "Duplicate sample for timestamp", "series", err)
					flag = false
					continue
				case storage.ErrOutOfBounds:
					level.Debug(a.logger).Log("msg", "Out of bounds metric", "series", err)
					flag = false
					continue
				case errSampleLimit:
					level.Debug(a.logger).Log("msg", "Sample Error", "series", err)
					flag = false
					continue
				}

				if !flag {
					delete(a.cache, hash)
					ref, err := app.Add(lbls, sample.Timestamp, sample.Value)
					if err != nil {
						level.Debug(a.logger).Log("call", "appender.Add", "err", err)
					}

					a.cache[hash] = ref
				}

			} else {
				ref, err := app.Add(lbls, sample.Timestamp, sample.Value)
				switch err {
				case nil:
					continue
				case storage.ErrNotFound:
					level.Debug(a.logger).Log("call", "appender.Add", "err", err)
					flag = false
				case storage.ErrOutOfOrderSample:
					level.Debug(a.logger).Log("msg", "Out of order sample", "series", err)
					flag = false
					continue
				case storage.ErrDuplicateSampleForTimestamp:
					level.Debug(a.logger).Log("msg", "Duplicate sample for timestamp", "series", err)
					flag = false
					continue
				case storage.ErrOutOfBounds:
					level.Debug(a.logger).Log("msg", "Out of bounds metric", "series", err)
					flag = false
					continue
				case errSampleLimit:
					level.Debug(a.logger).Log("msg", "Sample Error", "series", err)
					flag = false
					continue
				}

				a.cache[hash] = ref
			}
		}
	}

	err := app.Commit()
	if err != nil {
		level.Debug(a.logger).Log("call", "appender.Commit", "err", err)
		err = app.Rollback()
		if err != nil {
			level.Debug(a.logger).Log("call", "appender.Rollback", "err", err)
		}
	}
}

func (a *Adapter) getTimeSeries(mint, maxt int64, lbls []labels.Matcher) ([]*prompb.TimeSeries, error) {

	tss := make([]*prompb.TimeSeries, 0)
	var ts = &prompb.TimeSeries{}

	q, err := a.db.Querier(mint, maxt)
	if err != nil {
		level.Error(a.logger).Log("msg", "Get Querier failed", "err", err)
		return nil, err
	}

	series, err := q.Select(lbls...)
	if err != nil {
		level.Error(a.logger).Log("msg", "Select Series failed", "err", err)
		return nil, err
	}
	for {
		flag := series.Next()
		if flag == false {
			break
		}
		ss := series.At()
		lbs := ss.Labels()
		for _, lb := range lbs {
			ts.Labels = append(ts.Labels, &prompb.Label{lb.Name, lb.Value})
		}

		si := ss.Iterator()
		for {
			flag := si.Next()
			if flag == false {
				break
			}
			t, v := si.At()
			ts.Samples = append(ts.Samples, prompb.Sample{v, t})

		}
		tss = append(tss, ts)
	}
	return tss, nil
}

func convertLabel(lbls []*prompb.Label) labels.Labels{

	res := make([]labels.Label,0, len(lbls))
	for _, lb := range lbls {
		res = append(res, labels.Label{lb.Name, lb.Value})
	}

	return res
}

func (a *Adapter) convertLabelMatcher(matchers []*prompb.LabelMatcher) []labels.Matcher {
	lbs := make([]labels.Matcher, 0, len(matchers))
	var mat labels.Matcher
	for _, m := range matchers {
		switch m.Type {
		case prompb.LabelMatcher_EQ:
			mat = labels.NewEqualMatcher(m.Name, m.Value)
			lbs = append(lbs, mat)
		case prompb.LabelMatcher_RE:
			mat, err := labels.NewRegexpMatcher(m.Name, m.Value)
			if err != nil {
				level.Error(a.logger).Log("msg", "RegexpMatcher Error", "err", err)
			}
			lbs = append(lbs, mat)
		case prompb.LabelMatcher_NEQ:
			mat :=  &notMatcher{labels.NewEqualMatcher(m.Name, m.Value)}
			lbs = append(lbs, mat)
		case prompb.LabelMatcher_NRE:
			m, err := labels.NewRegexpMatcher(m.Name, m.Value)
			if err != nil {
				level.Error(a.logger).Log("msg", "NotRegexpMatcher Error", "err", err)
			}
			mat :=  &notRegexpMatcher{m}
			lbs = append(lbs, mat)
		}

	}

	return lbs
}

