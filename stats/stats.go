package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"gopkg.in/redis.v2"
)

type Project struct {
	Author      string `json:"author"`
	Title       string `json:"title"`
	Url         string `json:"url"`
	Points      int    `json:"points"`
	NumComments int    `json:"num_comments"`
	CreatedAtI  int    `json:"created_at_i"`
}

func main() {
	client := redis.NewTCPClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	v, err := client.HGetAllMap("authors:num_projects").Result()
	if err != nil {
		log.Printf("Error with get %s\n", err)
	}

  // bucket number of projects by users
	bucket := make(map[int][]string)
	for key, value := range v {
		count, _ := strconv.Atoi(value)
		bucket[count] = append(bucket[count], key)
	}

  // find the largest project count
	maxProjects := 0
	for numProjects, _ := range bucket {
		if numProjects > maxProjects {
			maxProjects = numProjects
		}
	}

  // find the highest scoring post
	maxPoints := 0
	var maxResult *Project
	for i := 1; i < maxProjects; i++ {
		results := StatsOnAuthors(client, bucket[i])
		for _, result := range results {
			if result.Points > maxPoints {
				maxPoints = result.Points
				maxResult = result
			}
		}
	}

	log.Printf("highest number of projects: %d \n", maxProjects)
	log.Printf("->most prolific authors: %#v \n", bucket[maxProjects])
	results := StatsOnAuthors(client, bucket[maxProjects])
	for _, result := range results {
		log.Printf("%s: %#v \n", result.Author, result.Title)
	}

	log.Printf("highest points: %d -> %#v \n", maxPoints, maxResult)
	r, _ := GetResultsByAuthor(client, maxResult.Author)
	for _, p := range r {
		log.Printf("-- %#v \n", p)
	}
}

func GetResultsByAuthor(client *redis.Client, username string) (results []*Project, err error) {
	lookup := fmt.Sprintf("author:blob:%s", username)
	v := client.LRange(lookup, 0, -1)
	projects, err := v.Result()

	if err != nil {
		log.Printf("error with lrange lookup \n")
	}

	for _, pproject := range projects {
		r := Project{}
		err := json.Unmarshal([]byte(pproject), &r)
		if err != nil {
			log.Printf("error json unmarshalling \n")
		}
		//log.Printf("[%d points] [%d comments] @%d title: %s \n", r.Points, r.NumComments, r.CreatedAtI, r.Title)
		results = append(results, &r)
	}
	return results, err
}

func StatsOnAuthors(client *redis.Client, authors []string) (projectResults []*Project) {
	for _, username := range authors {
		res, err := GetResultsByAuthor(client, username)
		if err != nil {
			continue
		}
		projectResults = append(projectResults, res...)
	}
	return projectResults
}
