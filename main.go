package lorenkadi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	goshopify "github.com/bold-commerce/go-shopify/v4"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/pkg/sftp"
	"github.com/xuri/excelize/v2"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func init() {
	functions.CloudEvent("executeShopifyUpdateShipping", executeShopifyUpdateShipping)
}

// MessagePublishedData contains the full Pub/Sub message
// See the documentation for more details:
// https://cloud.google.com/eventarc/docs/cloudevents#pubsub
type MessagePublishedData struct {
	Message PubSubMessage
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Payload struct {
	Date string `json:"date"`
}

func executeShopifyUpdateShipping(ctx context.Context, e event.Event) error {
	date := time.Now().Format("20060102")

	var msg MessagePublishedData
	if err := e.DataAs(&msg); err == nil {
		var payload Payload
		if err := json.Unmarshal(msg.Message.Data, &payload); err == nil && payload.Date != "" {
			if t, err := time.Parse("02/01/2006", payload.Date); err == nil {
				date = t.Format("20060102")
			} else {
				log.Printf("Invalid date format %q, expected dd/mm/yyyy, using today", payload.Date)
			}
		}
	}

	trackingNumbers, remoteFileName, tmpFilePath := GetTrackingNumberFromFtpServer(date)
	if tmpFilePath != "" {
		defer os.Remove(tmpFilePath)
	}

	if len(trackingNumbers) == 0 {
		log.Println("No tracking numbers found, skipping Shopify order update")
		return nil
	}
	if err := GetShopifyOrders(trackingNumbers); err != nil {
		log.Printf("Failed to process order fulfillments: %v", err)
	}

	if tmpFilePath != "" && remoteFileName != "" {
		bucketName := os.Getenv("GCS_BUCKET_NAME")
		if bucketName == "" {
			log.Println("GCS_BUCKET_NAME not set, skipping GCS upload")
		} else if err := uploadToGCS(ctx, bucketName, remoteFileName, tmpFilePath); err != nil {
			log.Printf("Failed to upload file to GCS: %v", err)
		} else {
			log.Printf("Successfully uploaded %s to gs://%s/%s", remoteFileName, bucketName, remoteFileName)
		}
	}

	return nil
}

func GetShopifyOrders(trackingNumbers map[string]string) error {
	shopifyKey := os.Getenv("SHOPIFY_KEY")
	shopifySecret := os.Getenv("SHOPIFY_SECRET")
	shopifyToken := os.Getenv("SHOPIFY_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	app := goshopify.App{
		ApiKey:    shopifyKey,
		ApiSecret: shopifySecret,
		Scope:     "read_orders, write_orders, write_assigned_fulfillment_orders, read_assigned_fulfillment_orders, write_fulfillments, read_fulfillments",
	}

	client, err := goshopify.NewClient(app, "c24ed1-4b", shopifyToken, goshopify.WithVersion("2025-07"))
	if err != nil {
		log.Fatalln("Erreur lors de la création du client shopify")
	}

	shop, err := client.Shop.Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get shop info: %w", err)
	}

	log.Printf("Processing unfulfilled orders (timezone: %s)", shop.IanaTimezone)

	// Fetch all unfulfilled orders
	orders, err := client.Order.List(ctx, &goshopify.OrderListOptions{FulfillmentStatus: "unfulfilled"})
	if err != nil {
		return fmt.Errorf("error fetching orders: %w", err)
	}

	log.Printf("Found %d orders to process", len(orders))

	// Process each order
	successCount := 0
	errorCount := 0

	for i, order := range orders {
		log.Printf("Processing order %d/%d (ID: %d)", i+1, len(orders), order.Id)

		if err := processOrder(ctx, client, order, trackingNumbers); err != nil {
			log.Printf("Error processing order %d: %v", order.Id, err)
			errorCount++
			continue
		}

		successCount++
	}

	log.Printf("Processing complete: %d successful, %d errors", successCount, errorCount)
	return nil
}

// processOrder handles the fulfillment creation for a single order
func processOrder(ctx context.Context, client *goshopify.Client, order goshopify.Order, trackingNumbers map[string]string) error {
	// Validate order has shipping lines
	if len(order.ShippingLines) == 0 {
		return fmt.Errorf("order has no shipping lines")
	}

	// Look up tracking number by order name, then by order number as fallback
	trackingID, exists := trackingNumbers[order.Name]
	if !exists {
		trackingID, exists = trackingNumbers[strconv.Itoa(order.OrderNumber)]
	}
	if !exists {
		log.Printf("No tracking number found for order %s (number: %d), skipping", order.Name, order.OrderNumber)
		return nil // Not an error, just skip
	}

	// Get fulfillment orders
	fulfillmentOrders, err := client.FulfillmentOrder.List(ctx, order.Id, nil)
	if err != nil {
		return fmt.Errorf("failed to get fulfillment orders: %w", err)
	}

	// Find pending fulfillment order
	fulfillmentOrderPending, err := findPendingFulfillmentOrder(fulfillmentOrders)
	if err != nil {
		return err
	}

	// Create fulfillment
	fulfillment := goshopify.Fulfillment{
		TrackingInfo: goshopify.FulfillmentTrackingInfo{
			Number:  trackingID,
			Company: "Colissimo",
		},
		LineItemsByFulfillmentOrder: []goshopify.LineItemByFulfillmentOrder{
			{
				FulfillmentOrderId: fulfillmentOrderPending.Id,
			},
		},
	}

	createdFulfillment, err := client.Fulfillment.Create(ctx, fulfillment)
	if err != nil {
		return fmt.Errorf("failed to create fulfillment: %w", err)
	}

	// Log success with formatted JSON
	if data, err := json.Marshal(createdFulfillment); err == nil {
		log.Printf("Successfully created fulfillment for order %d: %s", order.Id, string(data))
	}

	return nil
}

// findPendingFulfillmentOrder finds the first "open" fulfillment order
func findPendingFulfillmentOrder(fulfillmentOrders []goshopify.FulfillmentOrder) (*goshopify.FulfillmentOrder, error) {
	for _, fo := range fulfillmentOrders {
		if fo.Status == "open" {
			return &fo, nil
		}
	}
	return nil, fmt.Errorf("no pending fulfillment order found")
}

func GetTrackingNumberFromFtpServer(date string) (map[string]string, string, string) {
	userName := os.Getenv("FTP_USERNAME")
	password := os.Getenv("FTP_PASSWORD")
	host := os.Getenv("FTP_HOST")
	port := "22"

	// Create a known hosts callback from the environment variable
	hostKeyCallback, err := createHostKeyCallbackFromEnv()
	if err != nil {
		log.Fatalf("Failed to create host key callback: %v", err)
	}

	// Define the SSH configuration
	sshConfig := &ssh.ClientConfig{
		User: userName,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: hostKeyCallback,
	}

	sshConn, err := ssh.Dial("tcp", host+":"+port, sshConfig)
	if err != nil {
		log.Fatalf("Failed to dial: %v", err)
	}
	defer sshConn.Close()

	// Create an SFTP client
	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		log.Fatalf("Failed to create SFTP client: %v", err)
	}
	defer sftpClient.Close()
	//
	prefix := fmt.Sprintf("rcmd_%s", date)

	// ---- 5. List files in remote directory ----
	remoteDir := "out/"
	files, err := sftpClient.ReadDir(remoteDir)
	if err != nil {
		log.Fatal("Failed to read remote dir: ", err)
	}

	// ---- 6. Find file matching prefix ----
	var targetFile string
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), prefix) {
			targetFile = filepath.Join(remoteDir, f.Name())
			break
		}
	}

	if targetFile == "" {
		return make(map[string]string), "", ""
	}
	fmt.Println("Found file:", targetFile)

	// ---- 7. Download file ----
	remoteFile, err := sftpClient.Open(targetFile)
	if err != nil {
		log.Fatal("Failed to open remote file: ", err)
	}
	defer remoteFile.Close()

	tmpFile, err := os.CreateTemp("", "sftp-*.xlsx")
	if err != nil {
		log.Fatal("Failed to create temp file: ", err)
	}
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, remoteFile); err != nil {
		log.Fatal("Failed to copy file: ", err)
	}

	// ---- 8. Open with excelize ----
	xlFile, err := excelize.OpenFile(tmpFile.Name())
	if err != nil {
		log.Fatal("Failed to open Excel file: ", err)
	}

	// ---- 9. Read rows ----
	rows, err := xlFile.GetRows("rcmd")
	if err != nil {
		log.Fatal("Failed to read rows: ", err)
	}

	trackingNumbers := make(map[string]string)
	for _, row := range rows {
		codeta := row[2]
		if codeta == "E" {
			numol := row[8]
			trackingNumbers[numol] = row[4]
		}
	}

	return trackingNumbers, filepath.Base(targetFile), tmpFile.Name()
}

func uploadToGCS(ctx context.Context, bucketName, objectName, filePath string) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file for upload: %w", err)
	}
	defer f.Close()

	wc := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	if _, err := io.Copy(wc, f); err != nil {
		return fmt.Errorf("failed to write to GCS: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

func createHostKeyCallbackFromEnv() (ssh.HostKeyCallback, error) {
	// knownHostsData := os.Getenv("KNOWN_HOSTS")

	knownHostsData := "partenaires.groupe-novi.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGGv+XFTUzkxuv2yQVm60SzEkTQwg/eU2HSbk19jhBv/\npartenaires.groupe-novi.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCoNOs7mq+MAD5e8QeqzoVntPACnLX1R/nbMUqfJhFkHZZcIUdx1YQiAFnppmhJo70FO+9JUyFtQ9AHfhNz0kgEiGnUrGVYwIwHnSAhCI/u/iExhhAUhz7LQYlHERWcevMCuE4jQG3zd5T7IxPFcLHrunbIXXEiG1MG9ZxmpE03o1rrmwSzbx2Cdpsr7rKCJ5ppcXpL0VGV9U0Jd2ePVf17SuF9xU+rvpPhuigTnPf3pixlpJg9fsqy9G08KoNdoYY0DOuT9m3llDtdNIJNtJoTU7ACbY8WBsFt2yXilhtcM8LgIVVOBg0GIPy6o4e+h4dbWGQAltzc8zQeW4wybFw+TnuAgvbnCjApdUANrZWmCP1guwxYEGRHv/rO0AaDcL1yRzEVRPH2+vzvtW/59Q6wUWdscIVFNAfxm2VqpZuTpUggqByQmFf4sho4mRuqngWAL9DuCE0WGDsiarqZYB+QY/yanwMQ67KIAOaJ6xxtALIwmLWR1zl1RW7A4ofhZNk=\npartenaires.groupe-novi.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBJObfl0nkRYwKo5t0U41C/8WfkISb93pklu3jbq1SLYGIA7kURApAoCEWhIEQBRLPz6tiJQmFiqWH73Z0sH1yQA="

	if knownHostsData == "" {
		log.Fatalf("KNOWN_HOSTS environment variable not set")
	}

	tempFile, err := os.CreateTemp("", "known_hosts")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	_, err = tempFile.WriteString(knownHostsData)
	if err != nil {
		return nil, fmt.Errorf("failed to write known hosts data to temporary file: %v", err)
	}

	tempFile.Close()

	return knownhosts.New(tempFile.Name())
}
