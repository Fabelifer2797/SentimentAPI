/*
Compilation Commands:
go run main.go
CompileDaemon -command="Sentiment_API.exe"
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"code.sajari.com/docconv"
	"github.com/ledongthuc/pdf"

	//"github.com/gorilla/mux"
	"github.com/streadway/amqp"
)

//Structure types for holding data in execution time

type document struct {
	Name    string `json:Name`
	Content string `json:Content`
}

type documentList []document

var documentBank documentList

type analysisResult struct {
	Result internalResult `json:result`
}

type internalResult struct {
	Polarity float64 `json:polarity`
	Type     string  `json:type`
}

type resultSentimentLog struct {
	DocumentName  string  `json:documentName`
	PolarityFound float64 `json:polarityFound`
	SentimentType string  `json:sentimentType`
}

type sentimentsLogList []resultSentimentLog

var sentimentsLog sentimentsLogList

type brokerMessage struct{
	DocumentName string `json:DocumentName`
}

// Functions definitions

func indexRoute(w http.ResponseWriter, r *http.Request) {

	fmt.Fprintf(w, "Welcome to my Sentiment API, powered by Document Analyzer")

}

func getDocuments(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(documentBank)

}

func addDocument(w http.ResponseWriter, r *http.Request) {

	var newDocument document
	reqBody, err := ioutil.ReadAll(r.Body)

	if err != nil {
		fmt.Fprintf(w, "Insert a valid Document")
		return
	}

	json.Unmarshal(reqBody, &newDocument)
	documentBank = append(documentBank, newDocument)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newDocument)
	getSentiment(newDocument)
}

func getSentiment(documentReq document) {

	var result analysisResult
	var sentimentLog resultSentimentLog
	documentText := documentReq.Content

	postBody, _ := json.Marshal(map[string]string{
		"text": documentText,
	})

	responseBody := bytes.NewBuffer(postBody)
	resp, err := http.Post("https://sentim-api.herokuapp.com/api/v1/", "application/json", responseBody)

	if err != nil {
		log.Fatalf("An Error Occured %v", err)
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Fatalln(err)
		return
	}

	json.Unmarshal(body, &result)
	sentimentLog.DocumentName = documentReq.Name
	sentimentLog.PolarityFound = result.Result.Polarity
	sentimentLog.SentimentType = result.Result.Type
	sentimentsLog = append(sentimentsLog, sentimentLog)
	fmt.Println("Analysis Finished")
	fmt.Printf("%+v",sentimentsLog)

}

func getSentimentsLog(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sentimentsLog)
}

func getTextFromFile(fileNameComplete string) {

	var newDocument document
	var content string
	stringSplited := strings.Split(fileNameComplete, ".")
	fileName := stringSplited[0]
	fileExtension := stringSplited[1]

	if fileExtension == "txt" {

		content = getContentTxt("testDocuments/" + fileNameComplete)
		newDocument.Content = content
		newDocument.Name = fileName
		getSentiment(newDocument)

	} else if fileExtension == "pdf" {

		content = getContentPDF("testDocuments/" + fileNameComplete)
		newDocument.Content = content
		newDocument.Name = fileName
		getSentiment(newDocument)

	} else {
		
		content = getContentDocx("testDocuments/" + fileNameComplete)
		newDocument.Content = content
		newDocument.Name = fileName
		getSentiment(newDocument)
		
	}

}

func getContentTxt(fileNameComplete string) string {

	content, err := ioutil.ReadFile(fileNameComplete)

	if err != nil {
		log.Fatal(err)
	}

	return string(content)

}

func getContentPDF(fileNameComplete string) string {

	pdf.DebugOn = true
	content, err := readPdf(fileNameComplete) 
	if err != nil {
		panic(err)
	}
	return string(content)

}

func readPdf(path string) (string, error) {
	f, r, err := pdf.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var buf bytes.Buffer
    b, err := r.GetPlainText()
    if err != nil {
        return "", err
    }
    buf.ReadFrom(b)
	return buf.String(), nil
}


func getContentDocx(fileNameComplete string) string {

	content, err := docconv.ConvertPath(fileNameComplete)
	
	if err != nil {
		log.Fatal(err)
	}
	
	return content.Body
}

// RabbitMQ Functions

func failOnError(err error, msg string) {
	if err != nil {
	  log.Fatalf("%s: %s", msg, err)
	}
  }

func brokerListening(){

	var newBrokerMessage brokerMessage
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
			"Document-Analyzer",   // name
			"fanout", // type
			false,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
			"Sentiment-Queue",    // name
			true, // durable
			false, // delete when unused
			false,  // exclusive
			false, // no-wait
			nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
			q.Name, // queue name
			"Sentiment-Queue",     // routing key
			"Document-Analyzer", // exchange
			false,
			nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
			for d := range msgs {
					log.Printf(" [x] %s", d.Body)
					json.Unmarshal(d.Body, &newBrokerMessage)
					getTextFromFile(newBrokerMessage.DocumentName)
			}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever

}

func main() {

	brokerListening()
	
	/*

	//In case we need a API sever listening

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", indexRoute)
	router.HandleFunc("/documents", getDocuments).Methods("GET")
	router.HandleFunc("/documents", addDocument).Methods("POST")
	router.HandleFunc("/documents/analyzingResults", getSentimentsLog).Methods("GET")
	log.Fatal(http.ListenAndServe(":3000", router))
	
	*/
	

	

}
