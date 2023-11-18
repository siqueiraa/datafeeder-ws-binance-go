package main

import (
	"bot_test/db"
	"bot_test/util"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
	"golang.org/x/time/rate"
)

var wg sync.WaitGroup
var wgCandles sync.WaitGroup
var futuresClient *futures.Client
var cResults chan map[string]interface{}
var cCandlesResult chan []map[string]interface{}
var cSaveToDb chan []map[string]interface{}

var duration = time.Second * 60 / 600
var limiter = rate.NewLimiter(rate.Every(duration), 1)

func get_candles_rest(symbol, tf string, startTime float64) error {

	all_candles := make([]map[string]interface{}, 0)

	klines, err := futuresClient.NewKlinesService().Symbol(symbol).
		Interval(tf).Limit(1500).StartTime(int64(startTime)).Do(context.Background())

	if err != nil {
		fmt.Println(err)
		return err
	}

	for _, klineData := range klines {
		tz_time, err := util.Convert_ts_to_tz(klineData.OpenTime, "America/Sao_Paulo")

		if err != nil {
			fmt.Println("Error conversion time:", err)
			return err
		}

		map_to_Db := make(map[string]interface{})

		map_to_Db["time"] = tz_time
		map_to_Db["open"] = klineData.Open
		map_to_Db["high"] = klineData.High
		map_to_Db["low"] = klineData.Low
		map_to_Db["close"] = klineData.Close
		map_to_Db["volume"] = klineData.Volume
		map_to_Db["symbol"] = symbol
		map_to_Db["time_frame"] = tf

		all_candles = append(all_candles, map_to_Db)

	}

	cCandlesResult <- all_candles

	return nil

}

func get_candles(symbol, tf string) {
	wsKlineHandler := func(event *futures.WsKlineEvent) {
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
	_, _, err := futures.WsKlineServe(symbol, tf, wsKlineHandler, errHandler)
	if err != nil {
		fmt.Println("Error 2:", err)
		return
	}

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

func getMaxTimes(time_frame string) (map[string]float64, error) {
	// Execute the query
	query := fmt.Sprintf("SELECT symbol, (extract('epoch' from max(time))) * 1000 as max_time FROM candles_binance cb WHERE time_frame = '%s' GROUP BY symbol", time_frame)
	rows, err := db.FetchDataFromTable(query, &wg)
	if err != nil {
		return nil, err
	}

	// Create a map to store the results
	result := make(map[string]float64)

	// Iterate over the rows and populate the map
	for _, row := range rows {
		var symbol string

		// Modify the following lines according to your actual data structure
		// Example assuming your map has keys "symbol" and "max_time"
		symbolValue, ok := row["symbol"].(string)
		if !ok {
			return nil, errors.New("failed to convert symbol to string")
		}
		maxTimeValue, ok := row["max_time"].(float64)
		if !ok {
			return nil, errors.New("failed to convert max_time to int64")
		}

		symbol = symbolValue

		result[symbol] = maxTimeValue
	}

	return result, nil
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

func managerCandles(pairs []string) (max_time_ret time.Time, err error) {
	primaryKey := []string{"symbol", "time_frame", "time"}

	max_time, err := getMaxTimes("1m")

	if err != nil {
		log.Fatalf("Failed initial check: %v", err)
		panic("Error to get max time")
	}

	all_results_candles := make([]map[string]interface{}, 0)
	cCandlesResult = make(chan []map[string]interface{}, 20)

	go func() {
		for {
			select {
			case resultado, ok := <-cCandlesResult:
				if !ok {
					// The channel is closed, exit the goroutine
					return
				}
				all_results_candles = append(all_results_candles, resultado...)

			}
		}
	}()

	wgCandles.Add(len(pairs))
	for _, symbol := range pairs {
		go func(symbol string) {
			defer wgCandles.Done() // Ensure wg.Done() is called for each goroutine

			// Reserve a token
			reservation := limiter.Reserve()
			if !reservation.OK() {
				// Could not get a token, log an error and return
				fmt.Println("Rate limiter error: could not acquire token")
				return
			}

			// Sleep for the reservation delay, i.e., wait for the next available token

			log.Println(symbol, "Get delay", reservation.Delay())
			time.Sleep(reservation.Delay())

			max_time_symbol := max_time[symbol]

			// Call your get_candles function here
			get_candles_rest(symbol, "1m", max_time_symbol)
		}(symbol)
	}

	wgCandles.Wait()
	close(cCandlesResult)
	time.Sleep(100 * time.Millisecond)

	if len(all_results_candles) > 0 {

		inicio := time.Now()
		err = db.InsertBulkData(all_results_candles, "candles_binance", primaryKey)
		if err != nil {
			fmt.Println("Error insert:", err)
			return max_time_ret, err
		}
		fim := time.Now()
		// Calculate the time difference
		tempoDecorrido := fim.Sub(inicio).Seconds()
		formattedTime := fmt.Sprintf("%.3f", tempoDecorrido)

		// Print the elapsed time
		log.Println("Insert Time", formattedTime, "Registers:", len(all_results_candles))
		max_time_ret, err := util.GetMinTime(all_results_candles)

		if err != nil {
			fmt.Println("Error:", err)
			return max_time_ret, err
		}
		return max_time_ret, nil
	}
	return max_time_ret, nil
}

func initial_check(pairs []string) {

	max_time, err := managerCandles(pairs)
	if err != nil {
		log.Fatalf("Failed to initialize the database: %v", err)
	}

	// Get the current time
	currentTime := time.Now()

	// Calculate the time difference
	timeDifference := currentTime.Sub(max_time)
	minutesDifference := timeDifference.Minutes()

	// Print the time difference
	if minutesDifference > 120 {
		initial_check(pairs)
	} else {
		log.Printf("Time difference: %v Minutes, finish initial check!\n", minutesDifference)

	}
}

func main() {
	err := db.InitDB()
	if err != nil {
		log.Fatalf("Failed to initialize the database: %v", err)
	}

	cResults = make(chan map[string]interface{}, 20)
	cSaveToDb = make(chan []map[string]interface{})

	futuresClient = binance.NewFuturesClient("", "") // USDT-M Futures
	fmt.Println(binance.RateLimitIntervalMinute)

	pairs, _ := getPairs()
	initial_check(pairs)
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
