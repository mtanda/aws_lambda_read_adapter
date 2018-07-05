package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/prompb"
)

type config struct {
	listenAddr string
}

type timeSerie struct {
	Target     string
	Datapoints [][]int
}

type lambdaResult []timeSerie

func runQuery(region string, q *prompb.Query, logger log.Logger) ([]*prompb.TimeSeries, error) {
	result := []*prompb.TimeSeries{}

	cfg := &aws.Config{Region: aws.String(region)}
	sess := session.Must(session.NewSession(cfg))
	client := lambda.New(sess)

	startTime := time.Unix(int64(q.StartTimestampMs/1000), int64(q.StartTimestampMs%1000*1000))
	endTime := time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000))

	metricName := ""
	functionName := ""
	paramTarget := ""
	paramType := ""
	for _, m := range q.Matchers {
		if m.Name == "__name__" {
			metricName = m.Value
		}
		if m.Name == "functionName" {
			functionName = m.Value
		}
		if m.Name == "target" {
			paramTarget = m.Value
		}
		if m.Name == "type" {
			paramType = m.Value
		}
	}
	if functionName == "" {
		return nil, fmt.Errorf("no function name specified")
	}
	params := map[string]interface{}{
		"range": map[string]string{
			"from": startTime.Format(time.RFC3339),
			"to":   endTime.Format(time.RFC3339),
		},
		"targets": map[string]string{
			"target": paramTarget,
			"type":   paramType,
		},
	}
	payload, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	input := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}
	resp, err := client.Invoke(input)
	if err != nil {
		return nil, err
	}
	if *resp.StatusCode != 200 {
		return nil, err
	}

	var jsonResult lambdaResult
	err = json.Unmarshal(resp.Payload, &jsonResult)
	if err != nil {
		return nil, err
	}

	for _, serie := range jsonResult {
		ts := &prompb.TimeSeries{}
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "__name__", Value: metricName})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "functionName", Value: functionName})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "target", Value: serie.Target})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "type", Value: paramType})
		for _, datapoint := range serie.Datapoints {
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: float64(datapoint[0]), Timestamp: int64(datapoint[1])})
		}
		result = append(result, ts)
	}
	return result, nil
}

func GetDefaultRegion() (string, error) {
	var region string

	metadata := ec2metadata.New(session.New(), &aws.Config{
		MaxRetries: aws.Int(0),
	})
	if metadata.Available() {
		var err error
		region, err = metadata.Region()
		if err != nil {
			return "", err
		}
	} else {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	}

	return region, nil
}

func main() {
	var cfg config

	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9461", "Address to listen on for web endpoints.")
	flag.Parse()

	logLevel := promlog.AllowedLevel{}
	logLevel.Set("info")
	logger := promlog.New(logLevel)

	// set default region
	region, err := GetDefaultRegion()
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}

	srv := &http.Server{Addr: cfg.listenAddr}
	http.Handle("/metrics", prometheus.Handler())
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(req.Queries) != 1 {
			http.Error(w, "Can only handle one query.", http.StatusBadRequest)
			return
		}

		timeSeries, err := runQuery(region, req.Queries[0], logger)
		if err != nil {
			level.Error(logger).Log("err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{Timeseries: timeSeries},
			},
		}
		data, err := proto.Marshal(&resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	level.Info(logger).Log("msg", "Listening on "+cfg.listenAddr)
	if err := srv.ListenAndServe(); err != nil {
		level.Error(logger).Log("err", err)
	}
}
