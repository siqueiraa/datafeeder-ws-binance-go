package main

import (
	"bot_test/db"
	"bot_test/util"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2"
)

var wg sync.WaitGroup
var cResults chan map[string]interface{}
var cSaveToDb chan []map[string]interface{}

func get_candles(symbol, tf string) {
	wsKlineHandler := func(event *binance.WsKlineEvent) {
		tz_time_event, err := util.Convert_ts_to_tz(event.Time, "America/Sao_Paulo")

		if err != nil {
			fmt.Println("Error conversion event time:", err)
			return
		}

		tz_time, err := util.Convert_ts_to_tz(event.Kline.StartTime, "America/Sao_Paulo")

		if err != nil {
			fmt.Println("Error conversion event time:", err)
			return
		}

		map_to_Db := make(map[string]interface{})

		map_to_Db["event_time"] = tz_time_event
		map_to_Db["time"] = tz_time
		map_to_Db["open"] = event.Kline.Open
		map_to_Db["high"] = event.Kline.High
		map_to_Db["low"] = event.Kline.Low
		map_to_Db["close"] = event.Kline.Close
		map_to_Db["volume"] = event.Kline.Volume
		map_to_Db["symbol"] = event.Symbol
		map_to_Db["time_frame"] = event.Kline.Interval

		cResults <- map_to_Db

	}
	errHandler := func(err error) {
		fmt.Println("Error:", err)
	}
	doneC, _, err := binance.WsKlineServe(symbol, tf, wsKlineHandler, errHandler)
	if err != nil {
		fmt.Println("Error 2:", err)
		return
	}

	<-doneC
}

func waitResult() {
	all_results := make([]map[string]interface{}, 0)
	for {
		select {
		case resultado := <-cResults:
			all_results = append(all_results, resultado)
			all_results = util.KeepHistoryMinute(all_results)
			cSaveToDb <- all_results

		}
	}
}

func getPairs() ([]string, error) {
	//query := "select distinct symbol from open_interest WHERE time >= now()::timestamp - INTERVAL '60 minutes'"
	query := "select distinct symbol from open_interest"
	ret_query, err := db.FetchDataFromTable(query, &wg)
	if err != nil {
		return nil, err
	}

	// Create a slice to hold the extracted symbols
	symbols := make([]string, len(ret_query))

	// Extract symbols from the maps
	for i, m := range ret_query {
		if symbol, ok := m["symbol"].(string); ok {
			symbols[i] = symbol
		} else {
			// Handle the case where the "symbol" key is not a string
			symbols[i] = "" // or handle the error as needed
		}
	}

	// Return the resulting slice of symbols
	return symbols, nil
}

func insertToDbAsync(v []map[string]interface{}, cSaveToDb chan []map[string]interface{}, primaryKey []string) {
	newV := make([]map[string]interface{}, len(v))

	for i, item := range v {
		// Create a copy of the map before modifying it
		newMap := make(map[string]interface{})
		for key, value := range item {
			newMap[key] = value
		}

		// Delete the "event_time" key if needed
		delete(newMap, "event_time")

		newV[i] = newMap
	}

	inicio := time.Now()
	//err := db.InsertData(v, "candles_binance", primaryKey)
	err := db.InsertBulkData(newV, "candles_binance", primaryKey)
	fim := time.Now()

	if err != nil {
		fmt.Println("Error insert:", err)
		return
	}
	// Calculate the time difference
	tempoDecorrido := fim.Sub(inicio).Seconds()
	formattedTime := fmt.Sprintf("%.3f", tempoDecorrido)

	// Print the elapsed time
	log.Println("Insert Time", formattedTime, "Registers:", len(v))
	//fmt.Println(time.Now(), "Insert Time", tempoDecorrido, "Registros", len(v))

}

func saveToDb() {
	last_save := time.Now()
	last_save = last_save.Add(-1 * time.Minute)
	wait_finish := false

	primaryKey := []string{"symbol", "time_frame", "time"}
	for {
		select {
		case v := <-cSaveToDb:
			//fmt.Println(time.Now(), "Recebeu v tamanho", len(v))
			now := time.Now()
			diff := now.Sub(last_save).Seconds()
			if diff >= 10.0 && !wait_finish {
				last_save = time.Now()
				wait_finish = true
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							// Handle the panic, log it, or take appropriate action
							fmt.Println("Panic in insertToDbAsync:", r)
						}
						wait_finish = false
					}()
					insertToDbAsync(v, cSaveToDb, primaryKey)
					wait_finish = false
				}()

			}
		}
	}
}

func main() {
	err := db.InitDB()
	if err != nil {
		log.Fatalf("Failed to initialize the database: %v", err)
	}

	cResults = make(chan map[string]interface{}, 20)
	cSaveToDb = make(chan []map[string]interface{})

	pairs, _ := getPairs()
	wg.Add(2)

	fmt.Println(len(pairs), "Pares")

	for _, symbol := range pairs {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done() // Ensure wg.Done() is called for each goroutine

			// Call your get_candles function here
			get_candles(symbol, "1m")
		}(symbol)
	}

	go waitResult()
	go saveToDb()

	wg.Wait()

	fmt.Println("Finish main")

	defer func() {
		if r := recover(); r != nil {
			// Handle the panic, log it, or take appropriate action
			fmt.Println("Panic in main:", r)
		}
	}()

	// Infinite loop
	for {
		time.Sleep(10 * time.Second)
	}

}
