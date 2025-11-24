package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
)

type Transcription struct {
	Timestamp     time.Time
	FileName      string
	OriginalText  string
	CorrectedText string
}

type PageData struct {
	Transcriptions []Transcription
	Total          int
	LastUpdated    time.Time
	WatchDir       string
}

var (
	environment          string
	watchDir             string
	openaiAPIKey         string
	groupmeBotID         string
	webhookURL           string
	processingDelay      int
	maxMessageLength     int
	systemPrompt         string
	processedFiles       = make(map[string]bool)
	processedFilesMux    sync.Mutex
	tmpl                 *template.Template
	transcriptions       []Transcription
	transcriptionsMux    sync.Mutex
	maxTranscriptions    = 100 // Adjust as needed
	webServerPort        string
	openaiModel          string
	groupmeMessageSuffix string
)

func init() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file, using environment variables")
	}

	environment = os.Getenv("ENVIRONMENT")
	if environment == "" {
		environment = "dev"
	}

	watchDir = os.Getenv("WATCH_DIR")
	if watchDir == "" {
		watchDir = "./watched_directory"
	}

	openaiAPIKey = os.Getenv("OPENAI_API_KEY")
	if openaiAPIKey == "" {
		log.Fatal("Missing OpenAI API key. Set the OPENAI_API_KEY environment variable.")
	}

	groupmeBotID = os.Getenv("GROUPME_BOT_ID")
	if groupmeBotID == "" {
		log.Fatal("Missing GroupMe Bot ID. Set the GROUPME_BOT_ID environment variable.")
	}

	webhookURL = os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		webhookURL = "https://api.groupme.com/v3/bots/post"
	}

	processingDelayStr := os.Getenv("PROCESSING_DELAY")
	processingDelay = 1
	if processingDelayStr != "" {
		if val, err := strconv.Atoi(processingDelayStr); err == nil {
			processingDelay = val
		}
	}

	maxMessageLengthStr := os.Getenv("MAX_MESSAGE_LENGTH")
	maxMessageLength = 1000
	if maxMessageLengthStr != "" {
		if val, err := strconv.Atoi(maxMessageLengthStr); err == nil {
			maxMessageLength = val
		}
	}

	systemPrompt = os.Getenv("SYSTEM_PROMPT")
	if systemPrompt == "" {
		systemPrompt = "You are a highly specialized transcription assistant for public safety dispatch communications. Your task is to transcribe emergency radio transmissions with absolute precision. Ensure all unit identifiers, codes, locations, and technical terms are accurately captured. Apply correct spelling, punctuation, and capitalization. Do not include any irrelevant content, such as external links (e.g., websites), promotional messages, or information that was not explicitly communicated in the radio transmission. Ensure the transcription strictly reflects the call content as spoken."
	}

	webServerPort = os.Getenv("WEB_SERVER_PORT")
	if webServerPort == "" {
		webServerPort = "8080"
	}

	openaiModel = os.Getenv("OPENAI_MODEL")
	if openaiModel == "" {
		openaiModel = "gpt-4" // Default to GPT-4; change to "gpt-3.5-turbo" if needed
	}

	groupmeMessageSuffix = os.Getenv("GROUPME_MESSAGE_SUFFIX")
	if groupmeMessageSuffix == "" {
		groupmeMessageSuffix = " - https://calls.sussexcountyalerts.com/"
	}

	// Load the HTML template
	tmplPath := filepath.Join("templates", "transcriptions.html")
	var errTpl error
	tmpl, errTpl = template.ParseFiles(tmplPath)
	if errTpl != nil {
		log.Fatalf("Error parsing template %s: %v", tmplPath, errTpl)
	}
}

func main() {
	go watchDirectory(watchDir)
	startWebServer(webServerPort)
}

func watchDirectory(dir string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Error creating file watcher:", err)
	}
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					if strings.HasSuffix(strings.ToLower(event.Name), ".mp3") {
						handleNewFile(event.Name)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("Watcher error:", err)
			}
		}
	}()

	err = watcher.Add(dir)
	if err != nil {
		log.Fatal("Error adding directory to watcher:", err)
	}
	log.Println("Started watching directory:", dir)
	<-done
}

func handleNewFile(filePath string) {
	// Deduplication
	processedFilesMux.Lock()
	if processedFiles[filePath] {
		processedFilesMux.Unlock()
		log.Println("File already processed:", filePath)
		return
	}
	processedFiles[filePath] = true
	processedFilesMux.Unlock()

	log.Println("New MP3 file detected:", filePath)
	time.Sleep(time.Duration(processingDelay) * time.Second) // Wait to ensure file is fully written

	fileName := filepath.Base(filePath)
	transcription, err := transcribeAudio(filePath)
	if err != nil {
		log.Println("Error during transcription:", err)
		transcription = fmt.Sprintf("Transcription error: %v", err)
	}

	correctedTranscription, err := postProcessTranscription(transcription)
	if err != nil {
		log.Println("Error during post-processing:", err)
		correctedTranscription = transcription // Fallback to original transcription
	}

	// Store the transcription
	storeTranscription(fileName, transcription, correctedTranscription)

	sendToGroupMe(correctedTranscription, fileName)
}

func transcribeAudio(filePath string) (string, error) {
	// Preprocess the audio file: Apply band-pass filter
	filteredFilePath, err := preprocessAudio(filePath)
	if err != nil {
		return "", fmt.Errorf("audio preprocessing failed: %w", err)
	}
	defer os.Remove(filteredFilePath) // Clean up the temporary filtered audio file

	// Prepare the request to OpenAI API
	apiURL := "https://api.openai.com/v1/audio/transcriptions"

	// Open the filtered audio file
	audioFile, err := os.Open(filteredFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open audio file: %w", err)
	}
	defer audioFile.Close()

	// Create a buffer to hold the multipart form data
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Add the file field
	part, err := writer.CreateFormFile("file", filepath.Base(filteredFilePath))
	if err != nil {
		return "", fmt.Errorf("failed to create form file: %w", err)
	}
	_, err = io.Copy(part, audioFile)
	if err != nil {
		return "", fmt.Errorf("failed to copy audio file: %w", err)
	}

	// Add other form fields
	_ = writer.WriteField("model", "whisper-1")
	_ = writer.WriteField("response_format", "text")
	_ = writer.WriteField("language", "en")

	// Close the writer to finalize the form data
	err = writer.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", apiURL, &requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+openaiAPIKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Check for non-200 status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OpenAI API error: %s", string(bodyBytes))
	}

	// The response is plain text
	transcription := strings.TrimSpace(string(bodyBytes))

	log.Println("Transcription completed for:", filePath)

	return transcription, nil
}

func preprocessAudio(inputFilePath string) (string, error) {
	// Create a temporary file for the filtered audio
	tempDir := os.TempDir()
	outputFilePath := filepath.Join(tempDir, filepath.Base(inputFilePath)+".filtered.mp3")

	// Apply band-pass filter using FFmpeg
	cmd := exec.Command("ffmpeg", "-y", "-i", inputFilePath, "-af", "highpass=f=300, lowpass=f=3400", outputFilePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg error: %s", string(output))
	}

	return outputFilePath, nil
}

func postProcessTranscription(transcription string) (string, error) {
	// Prepare the request to OpenAI Chat Completion API
	apiURL := "https://api.openai.com/v1/chat/completions"

	// Prepare the request payload
	payload := map[string]interface{}{
		"model": openaiModel,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": transcription},
		},
		"temperature": 0.0,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", apiURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+openaiAPIKey)
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Check for non-200 status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OpenAI Chat API error: %s", string(bodyBytes))
	}

	// Parse the response
	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	err = json.Unmarshal(bodyBytes, &result)
	if err != nil {
		return "", fmt.Errorf("failed to parse JSON response: %w", err)
	}

	if len(result.Choices) == 0 || result.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("no content in response")
	}

	correctedTranscription := strings.TrimSpace(result.Choices[0].Message.Content)

	log.Println("Post-processing completed.")

	return correctedTranscription, nil
}

func sendToGroupMe(transcription, fileName string) {
	messageText := transcription + groupmeMessageSuffix + fileName
	numMessages := int(float64(len(messageText)) / float64(maxMessageLength))
	if len(messageText)%maxMessageLength != 0 {
		numMessages++
	}

	for i := 0; i < numMessages; i++ {
		start := i * maxMessageLength
		end := start + maxMessageLength
		if end > len(messageText) {
			end = len(messageText)
		}
		chunk := messageText[start:end]
		payload := map[string]string{
			"bot_id": groupmeBotID,
			"text":   chunk,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error marshaling payload:", err)
			continue
		}

		req, err := http.NewRequest("POST", webhookURL, bytes.NewReader(payloadBytes))
		if err != nil {
			log.Println("Error creating request:", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("Error sending message to GroupMe:", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			log.Println("Message sent to GroupMe:", chunk)
		} else {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("Failed to send message to GroupMe: %d - %s\n", resp.StatusCode, string(bodyBytes))
		}
	}
}

func storeTranscription(fileName, originalText, correctedText string) {
	transcriptionsMux.Lock()
	defer transcriptionsMux.Unlock()

	t := Transcription{
		Timestamp:     time.Now(),
		FileName:      fileName,
		OriginalText:  originalText,
		CorrectedText: correctedText,
	}

	transcriptions = append(transcriptions, t)

	// Keep only the last 'maxTranscriptions' records
	if len(transcriptions) > maxTranscriptions {
		transcriptions = transcriptions[len(transcriptions)-maxTranscriptions:]
	}
}

func startWebServer(port string) {
	http.HandleFunc("/", transcriptionsHandler)
	http.HandleFunc("/transcriptions", transcriptionsHandler)

	log.Printf("Starting web server on port %s...", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Error starting web server:", err)
	}
}

func transcriptionsHandler(w http.ResponseWriter, r *http.Request) {
	transcriptionsMux.Lock()
	defer transcriptionsMux.Unlock()

	var lastUpdated time.Time
	if len(transcriptions) > 0 {
		lastUpdated = transcriptions[len(transcriptions)-1].Timestamp
	}

	data := PageData{
		Transcriptions: transcriptions,
		Total:          len(transcriptions),
		LastUpdated:    lastUpdated,
		WatchDir:       watchDir,
	}

	err := tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, "Error rendering template", http.StatusInternalServerError)
		log.Println("Error executing template:", err)
	}
}
