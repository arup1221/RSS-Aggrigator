package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/arup1221/rssaggrigator/internal/database"
	"github.com/google/uuid"
)

func startScrapping(
	db *database.Queries,
	concurrency int,
	timeBewtweenRequest time.Duration,
) {
	log.Printf("Scrapping on %v goroutines every %s duration", concurrency, timeBewtweenRequest)
	ticker := time.NewTicker(timeBewtweenRequest)

	for ; ; <- ticker.C{
		feeds, err := db.GetNextFeedsToFetch(
			context.Background(),
			int32(concurrency),
		)
		if err != nil {
			log.Print("error fetching feeds: ", err)
			continue
		}
		wg := &sync.WaitGroup{}
		for _, feed := range feeds{
			wg.Add(1)
			go scrapeFeed(db, wg, feed)
		}
		wg.Wait()
	}
}

func scrapeFeed(db *database.Queries ,wg *sync.WaitGroup, feed database.Feed){
	defer wg.Done()

	_, err := db.MarkFeedAsFetched(context.Background(), feed.ID)
	if err != nil{
		log.Println("Error marking feed as feached:", err)
		return
	}

	rssFeed, err := urlToFeed(feed.Url)
	if err != nil{
		log.Println("Error fetching feed:", err)
		return
	}
	
	for _, item := range rssFeed.Channel.Item{
		// log.Print("Found post- ", item.Title, " on feed- ", feed.Name)
		description := sql.NullString{}
		if item.Description != ""{
			description.String = item.Description
			description.Valid = true
		}

		pubAt, err := time.Parse(time.RFC1123Z, item.PubDate)
		if err != nil {
			log.Printf("couldn't parse time date %v with err %v", item.PubDate, err)
			continue
		}

		_, errs := db.CreatePost(context.Background(),
		database.CreatePostParams{
			ID:            uuid.New(),
			CreatedAt:     time.Now().UTC(),
			UpdatedAt:     time.Now().UTC(),
			Title:         item.Title,
			Description:   description,
			PublishedAt:   pubAt,
			Url:           item.Link,
			FeedID:        feed.ID,
		})
		if errs != nil {
			if strings.Contains(errs.Error(), "duplicate key"){
				continue
			}
			log.Printf("Error creating post: %v", errs)
		}
	

	}
	log.Printf("Feed %s collected, %v posts found ", feed.Name, len(rssFeed.Channel.Item))
} 