package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/RENCI/GoUtils/Collections"
	"github.com/RENCI/GoUtils/Convert"
	"github.com/RENCI/GoUtils/FileSystem"
	"github.com/RENCI/GoUtils/Networking"
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
	fetchsize         *string
	curent_file_index = 1
	wg                sync.WaitGroup
	base_url          string
	scroll_url        string
	https             *bool
)

func main() {
	user = flag.String("user", "elastic", "ES username for basic auth")
	password = flag.String("password", "elastic", "ES password for basic auth")
	port = flag.String("port", "9200", "ES port")
	host = flag.String("host", "localhost", "ES host")
	index_name = flag.String("index", "", "index name")
	split = flag.Int("split", 1000, "split size")
	limit = flag.Int("limit", 0, "limit")
	timeout = flag.String("timeout", "1m", "timeout")
	fetchsize = flag.String("fetchsize", "1000", "fetch size")
	https = flag.Bool("https", true, "Use HTTPS")
	output_path = flag.String("output", "./output", "output path")

	flag.Parse()
	if *https == true {
		base_url = "https://" + *user + ":" + *password + "@" + *host + ":" + *port + "/"
	} else {
		base_url = "http://" + *user + ":" + *password + "@" + *host + ":" + *port + "/"
	}
	scroll_url = base_url + *index_name + "/_search?scroll=" + *timeout + "&size=" + *fetchsize

	println("ElasticSearchDump started")

	if *split == 0 {
		GetAndSaveInOneFile()

	} else {
		GetAndSaveInMultipleFiles()
	}
	// TODO: add scroll_id delete call
	println("ElasticSearchDump finished")

}

func GetAndSaveInOneFile() {
	scroll_id, all_items, err := GetFirstBatch(scroll_url)

	if err != nil {
		log.Fatal(err)
		return
	}

	i := 1
	println(Convert.IntToString(i) + " Loaded " + Convert.IntToString(all_items.Size()) + " items")
	i++
	for {
		items, err := GetNextBatch(base_url, scroll_id)
		items.ForEach(func(item interface{}) { all_items.Add(item) })

		if err != nil {
			log.Fatal(err)
		}
		if items.Size() == 0 {
			break
		}

		println(Convert.IntToString(i) + " Loaded " + Convert.IntToString(all_items.Size()) + " items")
		i++
	}
	println(all_items.Size())
	SaveToFile(all_items)
}

func GetAndSaveInMultipleFiles() {
	ch := make(chan any, *split)

	wg.Add(2)
	go func() {
		defer wg.Done()
		scroll_id, all_items, err := GetFirstBatch(scroll_url)

		if err != nil {
			log.Fatal(err)
			return
		}

		for {
			if all_items.Size() == 0 {
				break
			}
			all_items.ForEach(func(item any) {
				ch <- item
			})

			items, err := GetNextBatch(base_url, scroll_id)
			all_items = items

			if err != nil {
				log.Fatal(err)
			}

		}
		close(ch)
	}()
	go func() {
		defer wg.Done()
		all_items := Collections.NewList[any]()
		for item := range ch {
			all_items.Add(item)
			if all_items.Size() == *split {
				SaveToFile(all_items)
				all_items.Clear()
			}
		}
		if all_items.Size() > 0 {
			SaveToFile(all_items)
			all_items.Clear()
		}
	}()

	wg.Wait()
}

func SaveToFile(items Collections.List[any]) {
	output_path := FileSystem.Path.Combine(*output_path, *index_name+"_"+fmt.Sprintf("%06d", curent_file_index)+"_export"+".json")
	all_items_dict := map[string]any{"all_hits": items.ToSlice()}
	SaveDictToFileJson(all_items_dict, output_path)
	println(Convert.IntToString(curent_file_index) + " Saved to " + output_path)
	curent_file_index++
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

func GetNextBatch(base_url string, scroll_id string) (Collections.List[any], error) {
	all_items := Collections.NewList[any]()
	url_scroll := base_url + "_search/scroll"
	res, err := Networking.HttpPost(url_scroll, map[string]any{"scroll_id": scroll_id, "scroll": "1m"})
	if err != nil {
		return all_items, err
	}

	data, err2 := MapFromJson(res)
	if err2 != nil {
		return all_items, err2
	}

	GetItemsFromResults(data, all_items)

	return all_items, nil
}

func GetFirstBatch(url string) (string, Collections.List[any], error) {
	all_items := Collections.NewList[any]()
	res, _ := Networking.HttpGet(url)

	if res == nil {
		return "", all_items, fmt.Errorf("No response from ElasticSearch")
	}
	data, _ := MapFromJson(res)

	scroll_id := data["_scroll_id"].(string)
	println("scroll_id:" + scroll_id)

	GetItemsFromResults(data, all_items)

	return scroll_id, all_items, nil
}

func GetItemsFromResults(data map[string]any, all_items Collections.List[any]) {
	hits := data["hits"].(map[string]any)["hits"].([]any)
	for _, hit := range hits {
		all_items.Add(hit.(map[string]any)["_source"])
	}
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
