package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type config struct {
	hostname string
	port     int32
	username string
	password string
	database string
	table    string

	dialTimeout  int32
	writeTimeout int32
	readTimeout  int32

	tickerSeconds int32
}

func (c config) MySQLUri(db string) string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
		c.username, c.password,
		c.hostname, c.port,
		db,
		c.dialTimeout,
		c.readTimeout,
		c.writeTimeout,
	)
}

func main() {
	var initialize bool
	var hostname, username, password, database, table string
	var readers, writers, port int
	var readTimeout, writeTimeout, dialTimeout int
	var tickerSeconds int

	flag.BoolVar(&initialize, "i", false, "Initialize DB")

	flag.StringVar(&hostname, "h", "127.0.0.1", "Host")
	flag.StringVar(&username, "u", "user", "Username")
	flag.StringVar(&password, "p", "pass", "Password")
	flag.StringVar(&database, "d", "db", "Database")
	flag.StringVar(&table, "t", "table", "Table")

	flag.IntVar(&port, "P", 3306, "Port")
	flag.IntVar(&writers, "w", 1, "Writers")
	flag.IntVar(&readers, "r", 1, "Readers")
	flag.IntVar(&readTimeout, "rt", 30, "Read timeout")
	flag.IntVar(&writeTimeout, "wt", 30, "Write timeout")
	flag.IntVar(&dialTimeout, "dt", 30, "Dial timeout")
	flag.IntVar(&tickerSeconds, "ts", 1, "Ticker seconds")

	flag.Parse()

	cfg := config{
		hostname:      hostname,
		port:          int32(port),
		username:      username,
		password:      password,
		database:      database,
		table:         table,
		readTimeout:   int32(readTimeout),
		writeTimeout:  int32(writeTimeout),
		dialTimeout:   int32(dialTimeout),
		tickerSeconds: int32(tickerSeconds),
	}

	log.Printf("DSN: %s", cfg.MySQLUri(cfg.database))

	if err := initalizeDB(context.Background(), cfg); err != nil {
		log.Fatalf("failed to initialize db: %v", err)
	}

	var wg sync.WaitGroup
	cancellableCtx, cancelFunc := context.WithCancel(context.Background())

	for i := 0; i < writers; i++ {
		wg.Add(1)
		id := i

		go func() {
			defer wg.Done()
			writer(cancellableCtx, id, cfg)
		}()
	}

	for i := 0; i < readers; i++ {
		wg.Add(1)
		id := i

		go func() {
			defer wg.Done()
			reader(cancellableCtx, id, cfg)
		}()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	sig := <-c
	log.Printf("Received %+v", sig)
	cancelFunc()

	wg.Wait()
}

func initalizeDB(ctx context.Context, cfg config) error {
	db, err := sql.Open("mysql", cfg.MySQLUri("mysql"))
	if err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}
	db.SetConnMaxLifetime(time.Second * 10)
	db.SetConnMaxIdleTime(time.Second * 5)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("USE %s", cfg.database))
	if err != nil {
		return fmt.Errorf("failed to use database: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b VARCHAR(32))", cfg.table))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func randSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func writer(ctx context.Context, id int, cfg config) {
	db, err := sql.Open("mysql", cfg.MySQLUri(cfg.database))
	if err != nil {
		log.Printf("writer %d: failed to connect to db: %v", id, err)
	}
	db.SetConnMaxLifetime(time.Second * 10)
	db.SetConnMaxIdleTime(time.Second * 5)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.PingContext(ctx); err != nil {
		log.Printf("writer %d: failed to ping: %v", id, err)
		return
	}

	ticker := time.NewTicker(time.Duration(cfg.tickerSeconds) * time.Second)

	for range ticker.C {
		if errors.Is(ctx.Err(), context.Canceled) {
			log.Printf("writer %d: shutting down", id)
			return
		}

		var hostname string
		if err := db.QueryRowContext(ctx, "SELECT @@hostname").Scan(&hostname); err != nil {
			log.Printf("writer %d: failed to check hostname: %v", id, err)
			continue
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (b) VALUES (?)", cfg.table), randSeq(32))
		if err != nil {
			log.Printf("writer %d: failed to insert: %v\n", id, err)
			continue
		}

		log.Printf("writer %d: write succeed on %s", id, hostname)
	}
}

func reader(ctx context.Context, id int, cfg config) {
	db, err := sql.Open("mysql", cfg.MySQLUri(cfg.database))
	if err != nil {
		log.Printf("reader %d: failed to connect to db: %v", id, err)
	}
	db.SetConnMaxLifetime(time.Second * 10)
	db.SetConnMaxIdleTime(time.Second * 5)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.PingContext(ctx); err != nil {
		log.Printf("reader %d: failed to ping: %v", id, err)
		return
	}

	ticker := time.NewTicker(time.Duration(cfg.tickerSeconds) * time.Second)

	for range ticker.C {
		if errors.Is(ctx.Err(), context.Canceled) {
			log.Printf("reader %d: shutting down", id)
			return
		}

		var hostname string
		if err := db.QueryRowContext(ctx, "SELECT @@hostname").Scan(&hostname); err != nil {
			log.Printf("reader %d: failed to check hostname: %v", id, err)
			continue
		}

		query := fmt.Sprintf("SELECT id, b FROM %s ORDER BY rand() LIMIT 1", cfg.table)

		var rowId int
		var b string
		err = db.QueryRowContext(ctx, query).Scan(&rowId, &b)
		if err != nil {
			log.Printf("reader %d: failed to select: %v\n", id, err)
			continue
		}

		log.Printf("reader %d: read succeed on %s", id, hostname)
	}
}
