package main

import (
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/lib/pq"
	"strings"
	"time"
	"net/http"
	"crypto/md5"
	"io/ioutil"
	"encoding/json"
	"html/template"
	"strconv"
)

type Pic struct {
	Id   string
	Link string
}

func query_sources(database *sql.DB, sources []string) {
	c := make(chan string)
	for _, source := range sources {
		go func(document string) {
			for {
				doc, err := goquery.NewDocument(document)
				if err != nil {
					fmt.Println("Goquery error: " + err.Error())
					continue
				}
				doc.Find("body div.pi_body a.thumb_item").Each(func(i int, s *goquery.Selection) {
					data, e := s.Find("img").Attr("data-src_big")
					if e {
						c <- strings.Split(data, "|")[0]
					}
				})
				fmt.Println(time.Now())
				time.Sleep(3 * time.Minute)
			}
		}(source)
	}
	go func() {
		for link := range c {
			slug := get_md5(link)
			database.Exec(`INSERT INTO posts(slug, link) VALUES($1, $2) RETURNING id`, slug, link)
		}
	}()
}

func get_md5(link string) string {
	h := md5.New()
	resp, _ := http.Get(link)
	defer resp.Body.Close()
	data, _ := ioutil.ReadAll(resp.Body)
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func main() {
	page_size := 3
	db, err := sql.Open("postgres", "user=goagr dbname=goagr password=24268486555ss")
    defer db.Close()

	if err != nil {
		fmt.Println(err)
		return
	}
	db.SetMaxIdleConns(4)

	query_sources(db, []string{"http://vk.com/thesmolny", "http://vk.com/mdk"})

	rows, err := db.Query("SELECT id, link FROM posts")
	if err != nil {
		fmt.Println(err)
	}
	for rows.Next() {
		var id int
		var link string
		err = rows.Scan(&id, &link)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q, e := r.URL.Query()["q"]
		offset_posts := 0
		pics := make([]Pic, 0)
		if e {
			offset_id, _ := strconv.ParseInt(q[0], 10, 32)
			offset, _ := db.Query("SELECT COUNT(id) FROM posts WHERE id >= $1", offset_id)
			offset.Next()
			offset.Scan(&offset_posts)
		}
		rows, _ := db.Query("SELECT id, link FROM posts ORDER BY id DESC LIMIT $1 OFFSET $2", page_size, offset_posts)

		var id, link string
		for rows.Next() {
			err = rows.Scan(&id, &link)
			pics = append(pics, Pic{id, link})
		}
		if e {
			resp, _ := json.Marshal(pics)
			fmt.Fprintf(w, string(resp))

		} else {
			t, _ := template.ParseFiles("index.html")
			t.Execute(w, struct{Pictures []Pic}{pics})
		}
	})

	http.ListenAndServe(":4040", nil)
}
