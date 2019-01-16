package main

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"os"
	"path/filepath"
	"tsdb-adapter/adapter"
	"gopkg.in/alecthomas/kingpin.v2"
	"tsdb-adapter/config"
)


var (
	ad *adapter.Adapter
	cfg *config.Config
)


func main() {

	cfg = config.NewConfig()

	a := kingpin.New(filepath.Base(os.Args[0]), "The Adapter for prometheus tsdb")

	a.Version("tsdb-adapter")

	a.HelpFlag.Short('h')

	a.Flag("web.listen-address",
		"Address to listen on for API.").
		Default("0.0.0.0:9005").StringVar(&cfg.ListenAddr)

	a.Flag("tsdb.retention", "How long to retain samples in storage.").
		Default("15d").SetValue(&cfg.Retention)

	a.Flag("tsdb.min-block-duration", "Minimum duration of a data block before being persisted. For use in testing.").
		Hidden().Default("2h").SetValue(&cfg.MinBlockDuration)

	a.Flag("tsdb.max-block-duration",
		"Maximum duration compacted blocks may span. For use in testing. (Defaults to 10% of the retention period).").
		Hidden().PlaceHolder("<duration>").SetValue(&cfg.MaxBlockDuration)

	debugName := a.Flag("debug.name", "Name to add as prefix to log lines.").Hidden().String()
	logLevel := a.Flag("log.level", "Log filtering level.").
		Default("debug").Enum("error", "warn", "info", "debug")

	a.Arg("db.path", "The filepath of tsdb").Default("data/").StringVar(&cfg.Path)


	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	var logger log.Logger
	{
		var lvl level.Option
		switch *logLevel {
		case "error":
			lvl = level.AllowError()
		case "warn":
			lvl = level.AllowWarn()
		case "info":
			lvl = level.AllowInfo()
		case "debug":
			lvl = level.AllowDebug()
		default:
			panic("unexpected log level")
		}
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger = level.NewFilter(logger, lvl)


		if *debugName != "" {
			logger = log.With(logger, "name", *debugName)
		}

		logger = log.With(logger, "time", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	ad, err = adapter.NewAdapter(logger, cfg)
	if err != nil {
		level.Error(logger).Log("msg", "Starting Adapter failed", "err", err)
		return
	}

	level.Info(logger).Log("msg", "Starting Adapter successfully!")

	http.HandleFunc("/write", RemoteWrtie)
	http.HandleFunc("/read", RemoteRead)
	http.ListenAndServe(cfg.ListenAddr, nil)
}


func RemoteWrtie(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// resolve snappy
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// resolve json
	var req prompb.WriteRequest

	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// resolve data
	ad.RemoteWriter(req)
	if _, err := w.Write([]byte("ok")); err != nil {
		return
	}
}

func RemoteRead(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// snappy
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// resolve json
	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rawData := ad.RemoteReader(req)
	res, _ := proto.Marshal(rawData)
	// sender
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	compressed = snappy.Encode(nil, res)
	if _, err := w.Write(compressed); err != nil {
		return
	}
}

