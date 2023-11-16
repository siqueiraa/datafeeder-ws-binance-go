package util

import (
	"fmt"
	"reflect"
	"time"
)

func Convert_ts_to_tz(x int64, tz string) (time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, err
	}

	timestampSeconds := x / 1000
	t := time.Unix(timestampSeconds, 0).In(loc)

	return t, nil
}

func ArredondarParaMinuto(timestamp time.Time) time.Time {
	ano, mes, dia := timestamp.Date()
	hora, minuto, _ := timestamp.Clock()
	loc := timestamp.Location()

	// Arredondar para o início do minuto
	timestampArredondado := time.Date(ano, mes, dia, hora, minuto, 0, 0, loc)

	return timestampArredondado
}

func StructToMap(s interface{}) map[string]interface{} {
	valorStruct := reflect.ValueOf(s)

	if valorStruct.Kind() == reflect.Ptr {
		valorStruct = valorStruct.Elem()
	}

	tipoStruct := valorStruct.Type()
	mapa := make(map[string]interface{})

	for i := 0; i < valorStruct.NumField(); i++ {
		campo := valorStruct.Field(i)
		nomeCampo := tipoStruct.Field(i).Name
		valorCampo := campo.Interface()

		if campo.Kind() == reflect.Struct {
			// Se o campo for uma struct, chame a função recursivamente
			mapa[nomeCampo] = StructToMap(valorCampo)
		} else {
			mapa[nomeCampo] = valorCampo
		}
	}

	return mapa
}

func CalculateElapsedTime(inicio time.Time) string {
	fim := time.Now()
	tempoDecorrido := fim.Sub(inicio).Milliseconds()
	//formattedTime := fmt.Sprintf("%.3f", tempoDecorrido)
	formattedTime := fmt.Sprintf("%v", tempoDecorrido)
	return formattedTime
}

func KeepHistoryMinute(data []map[string]interface{}) []map[string]interface{} {
	// Use um mapa para rastrear o último registro de cada minuto para cada símbolo
	ultimosRegistros := make(map[string]map[string]interface{})

	// Itera sobre os dados e atualiza os registros
	for _, registro := range data {
		symbol := registro["symbol"].(string)
		timeStr := registro["event_time"].(time.Time)

		key := fmt.Sprintf("%s_%s", symbol, timeStr.Format("2006-01-02 15:04"))

		// Verifica se o registro já existe e se o novo registro é mais recente
		if registroAtual, ok := ultimosRegistros[key]; !ok || timeStr.After(registroAtual["event_time"].(time.Time)) {
			ultimosRegistros[key] = registro
		}
	}

	// Converte o mapa de registros em um slice de mapas
	var resultados []map[string]interface{}
	for _, registro := range ultimosRegistros {
		resultados = append(resultados, registro)
	}

	resultados = keepRecordsLast5Minutes(resultados)

	return resultados
}

func keepRecordsLast5Minutes(data []map[string]interface{}) []map[string]interface{} {
	results := []map[string]interface{}{}
	now := time.Now()

	for _, record := range data {
		eventTime, ok := record["event_time"].(time.Time)
		if !ok {
			// Ignore records without a valid "event_time" field
			continue
		}

		// Calculate the difference in minutes between the current time and the "event_time"
		diff := now.Sub(eventTime).Minutes()

		if diff <= 2 {
			results = append(results, record)
		}
	}

	return results
}
