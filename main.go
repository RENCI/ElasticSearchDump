// compile with
// CGO_ENABLED=0 go build -ldflags="-extldflags=-static" -o esdump .
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"

	"github.com/RENCI/GoUtils/Collections"
	"github.com/RENCI/GoUtils/Convert"
	"github.com/RENCI/GoUtils/FileSystem"
	"github.com/RENCI/GoUtils/Networking"
)

var (
	user               *string
	password           *string
	port               *string
	host               *string
	index_names        *string
	new_index_names    *string
	split              *int
	limit              *int
	path               *string
	timeout            *string
	fetchsize          *string
	current_file_index = 1
	wg                 sync.WaitGroup
	base_url           string
	action             *string
	https              *bool
	disabled_reindex   map[string]bool = map[string]bool{}
)

func main() {
	println("ElasticSearchDump started")

	user = flag.String("user", "elastic", "ES username for basic auth")
	password = flag.String("password", "elastic", "ES password for basic auth")
	port = flag.String("port", "9200", "ES port")
	host = flag.String("host", "localhost", "ES host")
	index_names = flag.String("index", "", "index names")
	new_index_names = flag.String("newindex", "", "new index names")
	split = flag.Int("split", 1000, "split size")
	limit = flag.Int("limit", 0, "limit")
	timeout = flag.String("timeout", "1m", "timeout")
	fetchsize = flag.String("fetchsize", "1000", "fetch size")
	https = flag.Bool("https", true, "Use HTTPS")
	path = flag.String("path", "./output", "path")
	action = flag.String("action", "", "export or import")

	flag.Parse()

	base_url = *user + ":" + *password + "@" + *host + ":" + *port + "/"
	if *https == true {
		base_url = "https://" + base_url
	} else {
		base_url = "http://" + base_url
	}

	if *action == "export" {
		indices := strings.Split(*index_names, " ")
		new_indices := strings.Split(*new_index_names, " ")

		for i, s := range indices {
			cur_index := strings.TrimSpace(s)
			new_cur_index := cur_index

			if *new_index_names != "" {
				// if new index name provided
				new_cur_index = strings.TrimSpace(new_indices[i])
			}

			if len(cur_index) == 0 {
				continue
			}

			scroll_url := base_url + cur_index + "/_search?scroll=" + *timeout + "&size=" + *fetchsize
			GetAndSaveInMultipleFiles(scroll_url, cur_index, new_cur_index)
		}
	} else if *action == "import" {
		files, err := GetFiles()

		if err != nil {
			log.Fatal(err)
			return
		}

		ImportFiles(files, base_url)
	} else {
		log.Fatal("no action specified")
	}

	println("ElasticSearchDump finished")

}

func DeleteIndex(index string) {
	status, body, err := Networking.HttpDelete(base_url + index)
	if err != nil {
		log.Fatal(err)
	}

	switch status {
	case 200:
		fmt.Println("Deleted:", string(body))
	case 404:
		fmt.Println("Index not found:", string(body))
	default:
		fmt.Printf("Unexpected status %d: %s\n", status, body)
	}
}

func ImportFiles(files Collections.List[FileSystem.FileInfo], base_url string) {
	defer func() {
		println("ENABLING INDEX REFRESH")
		for k, _ := range disabled_reindex {
			err := SetRefresh(k, "1m")
			if err != nil {
				log.Print(err)
			}
		}
	}()

	files.ForEach(func(item FileSystem.FileInfo) {
		println("Importing " + item.Path)
		items, err2 := GetDictFromFileJson(item)
		if err2 != nil {
			log.Fatal(err2)
			return
		}
		all_items := items["all_hits"].([]interface{})
		index_name := items["index_name"].(string)
		index_url := base_url + index_name

		for i, item := range all_items {
			println("Importing item #" + Convert.IntToString(i))
			if i == 1 {
				if _, ok := disabled_reindex[index_url]; !ok {
					println("DISABLING INDEX REFRESH")
					disabled_reindex[index_url] = true

					err2 := SetRefresh(index_url, "-1")
					if err2 != nil {
						log.Fatal(err2)
						return
					}
				}
			}
			err := PutItemsToIndex(index_url, item.(map[string]any))
			if err != nil {
				log.Fatal(err)
				return
			}
		}

	})
}

func SetRefresh(index_url string, interval string) error {
	url := index_url + "/_settings"
	payload := map[string]any{
		"index": map[string]any{
			"refresh_interval": interval,
		},
	}

	status, body, err := Networking.HttpPut(url, payload)
	if err != nil {
		return fmt.Errorf("request error refresh on %s: %w", index_url, err)
	}
	if status != 200 {
		return fmt.Errorf("failed to update refresh on %s (status %d): %s", index_url, status, body)
	}
	return nil
}

func PutItemsToIndex(index_url string, item map[string]any) error {
	cur_url := index_url + "/_create/" + url.QueryEscape(item["es___id"].(string))
	delete(item, "es___id")
	_, _, err := Networking.HttpPost(cur_url, item)
	if err != nil {
		return err
	}
	return nil
}

func GetAndSaveInMultipleFiles(scroll_url string, index_name string, new_index_name string) {
	ch := make(chan any, *split)

	wg.Add(2)
	go func() {
		defer wg.Done()
		scroll_id, all_items, err := GetFirstBatch(scroll_url)

		if err != nil {
			log.Print(index_name)
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
				SaveToFile(new_index_name, all_items)
				all_items.Clear()
			}
		}
		if all_items.Size() > 0 {
			SaveToFile(new_index_name, all_items)
			all_items.Clear()
		}
	}()

	wg.Wait()
}

func SaveToFile(index_name string, items Collections.List[any]) {
	output_path := FileSystem.Path.Combine(*path, fmt.Sprintf("%06d", current_file_index)+"_"+index_name+".json")
	all_items_dict := map[string]any{
		"index_name": index_name,
		"all_hits":   items.ToSlice(),
	}
	SaveDictToFileJson(all_items_dict, output_path)
	println(Convert.IntToString(current_file_index) + " Saved to " + output_path)
	current_file_index++
}

func GetFiles() (Collections.List[FileSystem.FileInfo], error) {
	di := FileSystem.DirectoryInfo_New(*path)
	files, err := di.GetFiles()

	if err != nil {
		log.Fatal(err)
		return files, err
	}

	log.Printf("Found %d files", files.Size())

	return files, nil
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

func GetDictFromFileJson(file FileSystem.FileInfo) (map[string]any, error) {
	bs, err := file.ReadBytes()
	jsonres, err := MapFromJson(bs)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return jsonres, nil
}

func GetNextBatch(base_url string, scroll_id string) (Collections.List[any], error) {
	all_items := Collections.NewList[any]()
	url_scroll := base_url + "_search/scroll"
	_, res, err := Networking.HttpPost(url_scroll, map[string]any{"scroll_id": scroll_id, "scroll": "1m"})
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
	_, res, err := Networking.HttpGet(url)

	if res == nil {
		return "", all_items, err
	}
	data, _ := MapFromJson(res)

	if data["_scroll_id"] == nil {

		return "", all_items, errors.New("Could not find scroll_id. Check index name")
	}

	scroll_id := data["_scroll_id"].(string)
	println("scroll_id:" + scroll_id)

	GetItemsFromResults(data, all_items)

	return scroll_id, all_items, nil
}

func GetItemsFromResults(data map[string]any, all_items Collections.List[any]) {
	hits := data["hits"].(map[string]any)["hits"].([]any)
	for _, hit := range hits {
		item := hit.(map[string]any)["_source"]
		item.(map[string]interface{})["es___id"] = hit.(map[string]any)["_id"].(string)
		all_items.Add((item))
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
