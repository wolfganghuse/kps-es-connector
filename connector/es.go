package connector

import (
	"context"
	"log"
	"encoding/json"
	"strconv"
	b64 "encoding/base64"

	elastic "github.com/olivere/elastic/v7"

	"github.com/nutanix/kps-connector-go-sdk/transport"
)



type streamMetadata struct {
	esIndex string
	esInstance  string
}

// mapToStreamMetadata translates the stream metadata into the corresponding streamMetadata struct
func mapToStreamMetadata(metadata map[string]interface{}) *streamMetadata {
	index := metadata["index"].(string)
	instance := metadata["instance"].(string)
	return &streamMetadata{
		esInstance:  instance,
		esIndex: index,
	}
}

type consumer struct {
	// TODO: Add the relevant client and fields
}

// producer consumes the data from the relevant client or service and publishes them to KPS data pipelines
func newConsumer() *consumer {
	// TODO: Add the relevant clients and fields
	return &consumer{}
}

// nextMsg wraps the logic for consuming iteratively a transport.Message
// from the relevant client or service
func (c *consumer) nextMsg() ([]byte, error) {
	// TODO: Add the logic for fetching the next message from the client or service
	return []byte{}, nil
}

// subscribe wraps the logic to connect or subscribe to the corresponding stream
// from the relevant client or service
func (c *consumer) subscribe(ctx context.Context, metadata *streamMetadata) error {
	// TODO: Create the relevant subscriptions / initiate connection to the client or service
	return nil
}

// producer produces data received from KPS data pipelines to the relevant client
type producer struct {
	transport transport.Client
	esClient  *elastic.Client
	index   string
}

func newProducer() *producer {
	// TODO: Add the relevant client and fields
	return &producer{}
}

func (p *producer) connect(ctx context.Context, metadata *streamMetadata) error {

	client, err :=  elastic.NewClient(elastic.SetURL(metadata.esInstance),
	elastic.SetSniff(false),
	elastic.SetHealthcheck(false))

	if err != nil {
		log.Println("Error initializing : ", err)
		return err
	}
	p.esClient = client
	p.index = metadata.esIndex
	return nil
}

// subscribeMsgHandler is a callback function that wraps the logic for producing a transport.Message
// from the data pipelines into the relevant client or service
func (p *producer) subscribeMsgHandler(message *transport.Message) {

	dataJSON, err := json.Marshal(message.Payload)
	if err != nil {
		log.Printf("unable to marshal: %s", err)	
	}
	jsb64 := string(dataJSON)
	jsuq , err := strconv.Unquote(jsb64)
	if err != nil {
		log.Printf("unable unquote: %s", err)	
	} 
	js , err := b64.StdEncoding.DecodeString(jsuq)
	if err != nil {
		log.Printf("unable decode b64: %s", err)	
	} 
	log.Printf(string(js))
	_, err = p.esClient.Index().
		Index(p.index).
		BodyJson((string(js))).
		Do(context.Background())

	if err != nil {
		log.Printf("unable to emit message to ES: %s", err)	
	} else {
		log.Printf("msg emitted: %s", string(message.Payload))	
	}
}