/*
Compilation Commands:
go run main.go
CompileDaemon -command="Sentiment_API.exe"
*/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"code.sajari.com/docconv"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/ledongthuc/pdf"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/streadway/amqp"
)

//Structure types for holding data in execution time

type documentS struct {
	Name    string `json:Name`
	Content string `json:Content`
}

type documentList []documentS

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

type mongoDocument struct{

	ID   primitive.ObjectID `bson:"_id,omitempty"`
	fileID    string `bson:"fileID,omitempty"`
	sentiment string `bson:"sentiment,omitempty"`
	offensive float64 	 `bson:"offensive,omitempty"`
	employees string `bson:"employees,omitempty"`

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

	var newDocument documentS
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

func getSentiment(documentReq documentS) {

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
	//Mongo DB Update
	
	var newInsertion mongoDocument
	newInsertion.fileID = documentReq.Name
	newInsertion.sentiment = result.Result.Type
	newInsertion.offensive = 0
	newInsertion.employees = ""
	sendDataToMongoDB(newInsertion)
	

}

func getSentimentsLog(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sentimentsLog)
}

func getTextFromFile(fileNameComplete string) {

	var newDocument documentS
	var content string
	stringSplited := strings.Split(fileNameComplete, ".")
	fileExtension := stringSplited[1]

	if fileExtension == "txt" {

		content = getContentTxt("testDocuments/" + fileNameComplete)
		newDocument.Content = content
		newDocument.Name = fileNameComplete
		getSentiment(newDocument)

	} else if fileExtension == "pdf" {

		content = getContentPDF("testDocuments/" + fileNameComplete)
		newDocument.Content = content
		newDocument.Name = fileNameComplete
		getSentiment(newDocument)

	} else {
		
		content = getContentDocx("testDocuments/" + fileNameComplete)
		newDocument.Content = content
		newDocument.Name = fileNameComplete
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

//Azure Blob storage functions


func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

func getFileFromAzureBlob(fileName string){

	accountName := "documentanalyzer2"
	accountKey  := "35oiYj9BMx99zwV+Wk4nAlnIUlTWLOENmnfGYp7Gij/QrTc4lXjTEPYjdEZsK49HUmVceLSdEiDcWl8sEJoEyA=="

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Create a random string for the quick start container
	containerName := "documents"

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	// Create the container
	fmt.Printf("Creating a container named %s\n", containerName)
	ctx := context.Background() // This example uses a never-expiring context
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	handleErrors(err)


	// List the container that we have created above
	fmt.Println("Listing the blobs in the container:")
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		handleErrors(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Print("	Blob name: " + blobInfo.Name + "\n")
		}
	}

	// Here's how to download the blob
	blobURL := containerURL.NewBlockBlobURL(fileName)
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil{
		log.Fatal(err)
	}

	// NOTE: automatically retries are performed if the connection fails
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// read the body into a buffer
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)
	handleErrors(err)

	// The downloaded blob data is in downloadData's buffer. :Let's print it
	createFileSystem(fileName,downloadedData.String()) 
	


}


func createFileSystem(fileNameComplete string, documentData string){

	stringSplited := strings.Split(fileNameComplete, ".")
	fileExtension := stringSplited[1]

	if fileExtension == "txt" {

		createTxt(fileNameComplete, documentData)
		fmt.Printf("Txt created\n")

	} else if fileExtension == "pdf" {
		createPdf(fileNameComplete,documentData)
		fmt.Printf("PDF created\n")

	} else {
		createDocx(fileNameComplete, documentData)
		fmt.Printf("Docx created\n")
	}

}


func createTxt(fileNameComplete string, documentData string){

	fileTxt, err := os.Create("./testDocuments/" + fileNameComplete)

    if err != nil {
        log.Fatal(err)
    }

    defer fileTxt.Close()

    _, err2 := fileTxt.WriteString(documentData)

    if err2 != nil {
        log.Fatal(err2)
    }

}

func createPdf(fileNameComplete string, documentData string){


	filePDF, err := os.Create("./testDocuments/" + fileNameComplete)

    if err != nil {
        log.Fatal(err)
    }

    defer filePDF.Close()

    _, err2 := filePDF.WriteString(documentData)

    if err2 != nil {
		log.Fatal(err2)
	}
}

func createDocx(fileNameComplete string, documentData string){

	
	fileDOCX, err := os.Create("./testDocuments/" + fileNameComplete)

    if err != nil {
        log.Fatal(err)
    }

    defer fileDOCX.Close()

    _, err2 := fileDOCX.WriteString(documentData)

    if err2 != nil {
		log.Fatal(err2)
	}

}



//MongoDB Conection function

func sendDataToMongoDB(newInsertion mongoDocument){

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://devroot:devroot@mongo:27017"))
    if err != nil {
        log.Fatal(err)
    }
    ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
    err = client.Connect(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect(ctx)

    quickstartDatabase := client.Database("DBdocs")
    documentsCollection := quickstartDatabase.Collection("documents")

	sentimentResult, err := documentsCollection.InsertOne(ctx, newInsertion)

	if err != nil {
    	panic(err)
	}
	fmt.Println(sentimentResult.InsertedID)


}



// RabbitMQ Functions

func failOnError(err error, msg string) {
	if err != nil {
	  log.Fatalf("%s: %s", msg, err)
	}
  }

func brokerListening(){

	
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq-server:5672/")
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
					//log.Printf(" [x] %s", d.Body)
					getFileFromAzureBlob(string(d.Body))
					getTextFromFile(string(d.Body))
			}
	}()

	log.Printf(" Waiting for messages from the broker. To exit press CTRL+C")
	<-forever

}

func main() {

	brokerListening()
	

}
