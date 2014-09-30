package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"gopkg.in/redis.v2"
)

const (
	BaseUrl  = "https://hn.algolia.com/api/v1/search_by_date?tags=show_hn"
	MaxPages = 50 // only maximum 50 from algolia
)

// API from https://hn.algolia.com/api
type Response struct {
	Hits []Hit `json:"hits"`
}

type Hit struct {
	CreatedAt    string `json:"created_at"`
	Title        string `json:"title"`
	Url          string `json:"url"`
	Author       string `json:"author"`
	Points       int    `json:"points"`
	StoryText    string `json:"story_text"`
	CommentText  string `json:"comment_text"`
	NumComments  int    `json:"num_comments"`
	CreatedAtInt int    `json:"created_at_i"`
	ObjectId     int    `json:"objectID"`
}

func ResultsByPage(i int, client *redis.Client) {
	url := fmt.Sprintf("%s&page=%d", BaseUrl, i)

	resp, _ := http.Get(url)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Printf("err - %s \n", err)
		return
	}

	r := Response{}
	json.Unmarshal(body, &r)
	log.Printf("r - %#v \n", r)

	for _, hit := range r.Hits {
		log.Printf("[%d] title: %s by %s \n", hit.CreatedAtInt, hit.Title, hit.Author)
		if err := client.HIncrBy("authors:num_projects", hit.Author, 1).Err(); err != nil {
			log.Printf("error incrementing @ authors:num_projects\n")
			continue
		}

		hitBlob, err := json.Marshal(hit)
		if err != nil {
			log.Printf("error marshalling hit\n")
			continue
		}

		author_key := fmt.Sprintf("author:blob:%s", hit.Author)
		if err := client.LPush(author_key, string(hitBlob)).Err(); err != nil {
			log.Printf("error pushing keys @ author_key \n")
			continue
		}
	}

}

func main() {
	client := redis.NewTCPClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	log.Println(pong, err)

	for i := 0; i < MaxPages; i++ {
		ResultsByPage(i, client)
	}
}

