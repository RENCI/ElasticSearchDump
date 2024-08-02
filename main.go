package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/RENCI/GoUtils/Collections"
	"github.com/RENCI/GoUtils/FileSystem"
	"io"
	"log"
	"net/http"
	"sync"
)

var (
	user              *string
	password          *string
	port              *string
	host              *string
	index_name        *string
	split             *int
	limit             *int
	output_path       *string
	timeout           *string
	size              *string
	curent_file_index = 1
	wg                sync.WaitGroup
)

func main() {
	user = flag.String("user", "elastic", "ES username for basic auth")
	password = flag.String("password", "elastic", "ES password for basic auth")
	port = flag.String("port", "9200", "ES port")
	host = flag.String("host", "localhost", "ES host")
	index_name = flag.String("index", "", "index name")
	split = flag.Int("split", 1000, "split size")
	limit = flag.Int("limit", 1115, "limit")
	timeout = flag.String("timeout", "1m", "timeout")
	size = flag.String("size", "100", "size")
	output_path = flag.String("output", "./", "output path")

	flag.Parse()

	if *split < 1 {
		log.Fatal("split parameter must be more than 1")
	}
	dumpFromES()
}

func dumpFromES() {

	println("ElasticSearchDump started")
	base_url := "https://" + *user + ":" + *password + "@" + *host + ":" + *port + "/"
	url := base_url + *index_name + "/_search?scroll=" + *timeout + "&size=" + *size

	ch := make(chan any, *split)

	total_loaded := 0
	all_items := Collections.NewList[any]()

	scroll_id := GetFirstBatch(url, all_items)
	total_loaded += all_items.Size()

	wg.Add(2)
	go SaveToFile(ch)
	go func() {
		defer wg.Done()
		if total_loaded > *limit {
			all_items = all_items.GetRange(0, *limit)
		}
		all_items.ForEach(func(item any) {
			ch <- item
		})

		all_items.Clear()

		for total_loaded < *limit {
			items_slice, err := GetNextBatch(base_url, scroll_id)

			if err != nil {
				log.Fatal(err)
			}
			all_items.AddRange(items_slice)
			new_total := total_loaded + all_items.Size()

			if new_total > *limit {
				all_items = all_items.GetRange(0, *limit-total_loaded)
				total_loaded = new_total
			}

			if len(items_slice) == 0 {
				break
			}
			all_items.ForEach(func(item any) {
				ch <- item
			})

			all_items.Clear()
		}
		close(ch)
	}()
	wg.Wait()
	// TODO: add scroll_id delete call
	println("ElasticSearchDump finished")
}

func SaveToFile(items chan any) {
	defer wg.Done()
	for c_item := range items {
		all_items := Collections.NewList[any]()
		all_items.Add(c_item)

		n_in_chan := len(items)
		for i := 1; i < *split && i <= n_in_chan; i++ {
			all_items.Add(<-items)
		}
		output_path := FileSystem.Path.Combine(*output_path, *index_name+"_"+fmt.Sprintf("%06d", curent_file_index)+"_export"+".json")
		all_items_dict := map[string]any{"all_hits": all_items.ToSlice()}
		SaveDictToFileJson(all_items_dict, output_path)
		curent_file_index++
	}

}

func SaveDictToFileJson(all_items_dict map[string]any, output_path string) {
	jsonres, err := MapToJson(all_items_dict)
	if err != nil {
		log.Fatal(err)
	}

	fi := FileSystem.FileInfo_New(output_path)
	err = fi.WriteBytes(jsonres)
	if err != nil {
		log.Fatal(err)
	}
}

func GetNextBatch(base_url string, scroll_id string) ([]any, error) {
	url_scroll := base_url + "_search/scroll"
	res, err := MakePostRequest(url_scroll, map[string]any{"scroll_id": scroll_id, "scroll": "1m"})
	if err != nil {
		return nil, err
	}

	data, err2 := MapFromJson(res)
	if err2 != nil {
		return nil, err2
	}

	if v, ok := data["hits"].(map[string]any); ok {
		hits := v["hits"].([]interface{})
		return hits, nil
	}

	return nil, nil
}

func GetFirstBatch(url string, all_items Collections.List[any]) string {
	res, _ := MakeGetRequest(url)
	data, _ := MapFromJson(res)

	scroll_id := data["_scroll_id"].(string)
	println("scroll_id:" + scroll_id)

	hits := data["hits"].(map[string]any)["hits"].([]any)
	all_items.AddRange(hits)
	return scroll_id
}

func MapFromJson(jsondata []byte) (map[string]any, error) {
	var data map[string]any
	err := json.Unmarshal(jsondata, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func MapToJson(data map[string]any) ([]byte, error) {
	res, err := json.MarshalIndent(data, "", "  ") //json.Marshal(data)
	return res, err
}

func MakeGetRequest(url string) ([]byte, error) {
	client := GetHttpClientWithNoTLSCheck()
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, err
}

func MakePostRequest(url string, reqbody map[string]any) ([]byte, error) {
	client := GetHttpClientWithNoTLSCheck()
	jsonValue, err := json.Marshal(reqbody)
	if err != nil {
		return nil, err
	}

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, err
}

func GetHttpClientWithNoTLSCheck() *http.Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	return client
}
