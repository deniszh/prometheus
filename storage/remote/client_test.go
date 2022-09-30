// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
)

var longErrMessage = strings.Repeat("error message", maxErrMsgLen)

func TestStoreHTTPErrorHandling(t *testing.T) {
	tests := []struct {
		code int
		err  error
	}{
		{
			code: 200,
			err:  nil,
		},
		{
			code: 300,
			err:  errors.New("server returned HTTP status 300 Multiple Choices: " + longErrMessage[:maxErrMsgLen]),
		},
		{
			code: 404,
			err:  errors.New("server returned HTTP status 404 Not Found: " + longErrMessage[:maxErrMsgLen]),
		},
		{
			code: 500,
			err:  RecoverableError{errors.New("server returned HTTP status 500 Internal Server Error: " + longErrMessage[:maxErrMsgLen]), defaultBackoff},
		},
	}

	for _, test := range tests {
		server := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, longErrMessage, test.code)
			}),
		)

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		conf := &ClientConfig{
			URL:     &config_util.URL{URL: serverURL},
			Timeout: model.Duration(time.Second),
		}

		hash, err := toHash(conf)
		require.NoError(t, err)
		c, err := NewWriteClient(hash, conf)
		require.NoError(t, err)

		err = c.Store(context.Background(), []byte{})
		if test.err != nil {
			require.EqualError(t, err, test.err.Error())
		} else {
			require.NoError(t, err)
		}

		server.Close()
	}
}

func TestClientRetryAfter(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, longErrMessage, 429)
		}),
	)
	defer server.Close()

	getClient := func(conf *ClientConfig) WriteClient {
		hash, err := toHash(conf)
		require.NoError(t, err)
		c, err := NewWriteClient(hash, conf)
		require.NoError(t, err)
		return c
	}

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	conf := &ClientConfig{
		URL:              &config_util.URL{URL: serverURL},
		Timeout:          model.Duration(time.Second),
		RetryOnRateLimit: false,
	}

	c := getClient(conf)
	err = c.Store(context.Background(), []byte{})
	_, ok := err.(RecoverableError)
	require.False(t, ok, "Recoverable error not expected.")

	conf = &ClientConfig{
		URL:              &config_util.URL{URL: serverURL},
		Timeout:          model.Duration(time.Second),
		RetryOnRateLimit: true,
	}

	c = getClient(conf)
	err = c.Store(context.Background(), []byte{})
	_, ok = err.(RecoverableError)
	require.True(t, ok, "Recoverable error was expected.")
}

func TestRetryAfterDuration(t *testing.T) {
	tc := []struct {
		name     string
		tInput   string
		expected model.Duration
	}{
		{
			name:     "seconds",
			tInput:   "120",
			expected: model.Duration(time.Second * 120),
		},
		{
			name:     "date-time default",
			tInput:   time.RFC1123, // Expected layout is http.TimeFormat, hence an error.
			expected: defaultBackoff,
		},
		{
			name:     "retry-after not provided",
			tInput:   "", // Expected layout is http.TimeFormat, hence an error.
			expected: defaultBackoff,
		},
	}
	for _, c := range tc {
		require.Equal(t, c.expected, retryAfterDuration(c.tInput), c.name)
	}
}

func TestReadClient(t *testing.T) {
	tests := []struct {
		name                  string
		httpHandler           http.HandlerFunc
		expectedLabels        []map[string]string
		expectedSamples       [][]model.SamplePair
		expectedErrorContains string
		sortSeries            bool
	}{
		{
			name:        "sorted sampled response",
			httpHandler: sampledResponseHTTPHandler(t),
			expectedLabels: []map[string]string{
				{"foo1": "bar"},
				{"foo2": "bar"},
			},
			expectedSamples: [][]model.SamplePair{
				{
					{Timestamp: model.Time(0), Value: model.SampleValue(3)},
					{Timestamp: model.Time(5), Value: model.SampleValue(4)},
				},
				{
					{Timestamp: model.Time(0), Value: model.SampleValue(1)},
					{Timestamp: model.Time(5), Value: model.SampleValue(2)},
				},
			},
			expectedErrorContains: "",
			sortSeries:            true,
		},
		{
			name:        "unsorted sampled response",
			httpHandler: sampledResponseHTTPHandler(t),
			expectedLabels: []map[string]string{
				{"foo2": "bar"},
				{"foo1": "bar"},
			},
			expectedSamples: [][]model.SamplePair{
				{
					{Timestamp: model.Time(0), Value: model.SampleValue(1)},
					{Timestamp: model.Time(5), Value: model.SampleValue(2)},
				},
				{
					{Timestamp: model.Time(0), Value: model.SampleValue(3)},
					{Timestamp: model.Time(5), Value: model.SampleValue(4)},
				},
			},
			expectedErrorContains: "",
			sortSeries:            false,
		},
		{
			name: "chunked response",
			httpHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

				flusher, ok := w.(http.Flusher)
				require.True(t, ok)

				cw := NewChunkedWriter(w, flusher)
				l := []prompb.Label{
					{Name: "foo", Value: "bar"},
				}

				chunks := buildTestChunks(t)
				for i, c := range chunks {
					cSeries := prompb.ChunkedSeries{Labels: l, Chunks: []prompb.Chunk{c}}
					readResp := prompb.ChunkedReadResponse{
						ChunkedSeries: []*prompb.ChunkedSeries{&cSeries},
						QueryIndex:    int64(i),
					}

					b, err := proto.Marshal(&readResp)
					require.NoError(t, err)

					_, err = cw.Write(b)
					require.NoError(t, err)
				}
			}),
			expectedLabels: []map[string]string{
				{"foo": "bar"},
				{"foo": "bar"},
				{"foo": "bar"},
			},
			// This is the output of buildTestChunks
			expectedSamples: [][]model.SamplePair{
				{
					{Timestamp: model.Time(0), Value: model.SampleValue(0)},
					{Timestamp: model.Time(1000), Value: model.SampleValue(1)},
					{Timestamp: model.Time(2000), Value: model.SampleValue(2)},
					{Timestamp: model.Time(3000), Value: model.SampleValue(3)},
					{Timestamp: model.Time(4000), Value: model.SampleValue(4)},
				},
				{
					{Timestamp: model.Time(5000), Value: model.SampleValue(1)},
					{Timestamp: model.Time(6000), Value: model.SampleValue(2)},
					{Timestamp: model.Time(7000), Value: model.SampleValue(3)},
					{Timestamp: model.Time(8000), Value: model.SampleValue(4)},
					{Timestamp: model.Time(9000), Value: model.SampleValue(5)},
				},
				{
					{Timestamp: model.Time(10000), Value: model.SampleValue(2)},
					{Timestamp: model.Time(11000), Value: model.SampleValue(3)},
					{Timestamp: model.Time(12000), Value: model.SampleValue(4)},
					{Timestamp: model.Time(13000), Value: model.SampleValue(5)},
					{Timestamp: model.Time(14000), Value: model.SampleValue(6)},
				},
			},
			expectedErrorContains: "",
		},
		{
			name: "unsupported content type",
			httpHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "foobar")
			}),
			expectedErrorContains: "unsupported content type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.httpHandler)
			defer server.Close()

			u, err := url.Parse(server.URL)
			require.NoError(t, err)

			conf := &ClientConfig{
				URL:              &config_util.URL{URL: u},
				Timeout:          model.Duration(5 * time.Second),
				ChunkedReadLimit: config.DefaultChunkedReadLimit,
			}
			c, err := NewReadClient("test", conf)
			require.NoError(t, err)

			// Query doesn't matter because we ultimately control what the server returns w/ test.httpHandler.
			query := &prompb.Query{}

			ss, err := c.Read(context.Background(), query, test.sortSeries)
			if test.expectedErrorContains != "" {
				require.ErrorContains(t, err, test.expectedErrorContains)
				return
			}

			require.NoError(t, err)

			i := 0

			for ss.Next() {
				require.NoError(t, ss.Err())
				s := ss.At()

				l := s.Labels()
				require.Equal(t, len(test.expectedLabels[i]), l.Len())
				for k, v := range test.expectedLabels[i] {
					require.True(t, l.Has(k))
					require.Equal(t, v, l.Get(k))
				}

				it := s.Iterator()
				j := 0

				for it.Next() {
					require.NoError(t, it.Err())

					ts, v := it.At()
					expectedSample := test.expectedSamples[i][j]

					require.Equal(t, int64(expectedSample.Timestamp), ts)
					require.Equal(t, float64(expectedSample.Value), v)

					j++
				}

				require.Equal(t, len(test.expectedSamples[i]), j)

				i++
			}

			require.NoError(t, ss.Err())
		})
	}
}

func sampledResponseHTTPHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")

		resp := prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "foo2", Value: "bar"},
							},
							Samples: []prompb.Sample{
								{Value: float64(1), Timestamp: int64(0)},
								{Value: float64(2), Timestamp: int64(5)},
							},
							Exemplars: []prompb.Exemplar{},
						},
						{
							Labels: []prompb.Label{
								{Name: "foo1", Value: "bar"},
							},
							Samples: []prompb.Sample{
								{Value: float64(3), Timestamp: int64(0)},
								{Value: float64(4), Timestamp: int64(5)},
							},
							Exemplars: []prompb.Exemplar{},
						},
					},
				},
			},
		}
		b, err := proto.Marshal(&resp)
		require.NoError(t, err)

		_, err = w.Write(snappy.Encode(nil, b))
		require.NoError(t, err)
	}
}
