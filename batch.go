package main

import (
	"context"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/PuerkitoBio/goquery"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/shurcooL/githubv4"
	"golang.org/x/oauth2"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	confDir             = "./config/env/"
	logFile             = "batch.log"
	repository_base_url = "https://github.com"
)

type (
	Config struct {
		Database DbConfig
	}

	DbConfig struct {
		Driver    string
		Host      string
		Port      string
		User      string
		Password  string
		Database  string
		Charset   string
		ParseTime string
	}

	Coin struct {
		Id           int `gorm:"primary_key"`
		Name         string
		Symbol       string
		Owner        string
		Repositories []*Repository `gorm:"foreignkey:CoinId;association_foreignkey:ID"`
		UpdatedAt    time.Time
		CreatedAt    time.Time
	}

	Repository struct {
		Id                          int `gorm:"primary_key"`
		CoinId                      int
		Coin                        Coin
		Name                        string
		Language                    string
		PullRequestsCount           int
		WatchersCount               int
		StargazersCount             int
		IssuesCount                 int
		CommitsCountForTheLastWeek  int
		CommitsCountForTheLastMonth int
		CommitsCount                int
		ContributorsCount           int
		UpdatedAt                   time.Time
		CreatedAt                   time.Time
	}
)

func (c Config) Db() (string, string) {
	return c.Database.Driver, c.Database.DSN()
}

func (d DbConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=%s",
		d.User,
		d.Password,
		d.Host,
		d.Port,
		d.Database,
		d.Charset,
		d.ParseTime)
}

func dbConnect() *gorm.DB {
	environment := os.Getenv("ENVIRONMENT")
	if environment == "" {
		log.Fatal("Failed to get application mode, check whether ENVIRONMENT is set.")
	}

	config := readConfig(environment)

	db, err := gorm.Open(config.Db())
	if err != nil {
		log.Fatal(err.Error())
	}
	return db
}

func readConfig(environment string) Config {
	var config Config
	confPath := confDir + environment + ".toml"
	_, err := toml.DecodeFile(confPath, &config)
	if err != nil {
		log.Fatal("Failed to read the Config.")
	}

	config.Database.Password = os.Getenv("DB_PASSWORD")

	return config
}

func commitsCountForTheLastWeek(n []struct{ CommittedDate string }, now time.Time) int {
	var count int
	aWeekago := now.AddDate(0, 0, -7).UTC().Format(time.RFC3339)

	for _, v := range n {
		if aWeekago <= v.CommittedDate {
			count++
		}
	}

	return count
}

func commitsCountForTheLastMonth(s []struct{ CommittedDate string }) int {
	return len(s)
}

func githubv4Client() *githubv4.Client {
	src := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: os.Getenv("GITHUB_TOKEN")},
	)
	httpClient := oauth2.NewClient(context.Background(), src)

	return githubv4.NewClient(httpClient)
}

func loggingSettings() {
	logfile, _ := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	multiLogFile := io.MultiWriter(os.Stdout, logfile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(multiLogFile)
}

var query struct {
	Repository struct {
		PullRequests struct {
			TotalCount int
		}
		Stargazers struct {
			TotalCount int
		}
		Watchers struct {
			TotalCount int
		}
		Issues struct {
			TotalCount int
		}
		PrimaryLanguage struct {
			Name string
		}
		DefaultBranchRef struct {
			Name   string
			Target struct {
				Commit struct {
					History struct {
						TotalCount int
						Nodes      []struct {
							CommittedDate string
						}
					} `graphql:"history(since: $since)"`
				} `graphql:"... on Commit"`
			}
		}
	} `graphql:"repository(owner: $owner, name: $name)"`
}

func main() {
	var err error
	db := dbConnect()
	defer db.Close()
	now := time.Now()

	loggingSettings()

	rows, err := db.Model(&Repository{}).Rows()
	if err != nil {
		log.Fatal("Failed to read the DB.")
	}

	for rows.Next() {
		var repo Repository
		var coin Coin
		var numbers []int

		db.ScanRows(rows, &repo)
		db.Model(&repo).Related(&coin)

		// GithubAPI V4
		variables := map[string]interface{}{
			"owner": githubv4.String(coin.Owner),
			"name":  githubv4.String(repo.Name),
			"since": githubv4.GitTimestamp{now.AddDate(0, -1, 0)},
		}

		err = githubv4Client().Query(context.Background(), &query, variables)
		if err != nil {
			log.Println(err)
			log.Println("API ERROR. CoinId: " + strconv.Itoa(coin.Id))
			continue
		}
		nodes := query.Repository.DefaultBranchRef.Target.Commit.History.Nodes

		// Web Scraping (commits and contributors count
		doc, err := goquery.NewDocument(repository_base_url + "/" + coin.Owner + "/" + repo.Name)
		if err != nil {
			log.Println("Scraping ERROR. CoinId: " + strconv.Itoa(coin.Id))
			continue
		}

		doc.Find("span.text-emphasized").Each(func(_ int, s *goquery.Selection) {
			text, _ := strconv.Atoi(strings.Replace(strings.TrimSpace(s.Text()), ",", "", -1))
			numbers = append(numbers, text)
		})

		if len(numbers) != 5 {
			log.Println("Scraping ERROR. CoinId: " + strconv.Itoa(coin.Id))
			continue
		}

		db.Model(&repo).Updates(Repository{
			Language:                    query.Repository.PrimaryLanguage.Name,
			PullRequestsCount:           query.Repository.PullRequests.TotalCount,
			WatchersCount:               query.Repository.Watchers.TotalCount,
			StargazersCount:             query.Repository.Stargazers.TotalCount,
			IssuesCount:                 query.Repository.Issues.TotalCount,
			CommitsCountForTheLastWeek:  commitsCountForTheLastWeek(nodes, now),
			CommitsCountForTheLastMonth: commitsCountForTheLastMonth(nodes),
			CommitsCount:                numbers[0],
			ContributorsCount:           numbers[4],
		})
	}
	log.Println("complate!")
}
